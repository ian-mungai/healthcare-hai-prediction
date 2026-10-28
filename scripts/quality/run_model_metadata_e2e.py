"""E2E for the model metadata check: the hook blocks each bad sample for its intended cause and passes the good one.

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_model_metadata_e2e``. Each case builds a scratch Git repository
with this repository's hook configuration, the checker and a small dbt project, commits it, applies the case's change, stages it and
runs the ``model-metadata`` hook through ``pre-commit``, so a hook that is not wired in cannot pass. It then runs the checker directly,
because pre-commit reports every failed hook as exit 1 and the checker's own exit code (0, 1 or 2) is part of the contract. A blocking
case passes only when both outputs cite its rule and its cause. ``--checker`` and ``--hook-config`` run the cases against another copy of
the checker or of the hook configuration, for example a mutant, and ``--case`` runs only the named cases. The report goes to
data/e2e/model_metadata/<UTC>_<pid>/report.json with the revision, the SHA-256 of the checker and the hook configuration and the tool versions.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, clear_git_environment, run_command
from scripts.quality.hook_stage import hook_stage

ROOT = Path(__file__).resolve().parents[2]
PRE_COMMIT = ROOT / ".venv" / "bin" / "pre-commit"
CHECKER = ROOT / "scripts" / "quality" / "model_metadata.py"
HOOK = "model-metadata"
CASE_SECONDS = 180
MAX_AUTO_WORKERS = 4
PROJECT = "dbt/dbt_project.yml"
SOURCES = "dbt/models/silver/staging/_sources.yml"
STAGING = "dbt/models/silver/staging/_staging__models.yml"
INTERMEDIATE = "dbt/models/silver/intermediate/_intermediate__models.yml"
DIMENSIONS = "dbt/models/gold/dimensions/_dimensions__models.yml"
FACTS = "dbt/models/gold/facts/_facts__models.yml"
MEASURES = "dbt/models/gold/dimensions/dim_measure.sql"
MACROS = "dbt/macros/readers.sql"
CONTRACT = "data_contracts/lakehouse/gold_contract.json"
BRONZE_MAP = "config/lakehouse/bronze_tables.json"
INT_MIXED = "dbt/models/silver/intermediate/int_mixed.sql"
LINEAGE_SQL = "dbt/models/silver/staging/stg_bronze__file_copies.sql"
OTHER_SOURCES = "dbt/models/silver/staging/zz_other_sources.yml"
PUBLIC = "    meta:\n      classification: Public\n"
CONFIDENTIAL = "    meta:\n      classification: Confidential\n"
ONC_PROPOSALS = {
    "onc_pi_attestations_csv": ("npi", "specialty"),
    "onc_pi_chpl_linkage_csv": ("facility_id", "facility_name", "address", "city_town", "zip_code", "telephone_number", "developer_name"),
}
ALL_COLUMNS_REASON = (
    "Every publisher column is confidential until an ONC publisher dictionary is stored; loader lineage _object_key and _member_sha256 are exempt (A273)."
)
LINEAGE_ENTRY = "  - name: stg_bronze__file_copies\n    description: Staged lineage.\n    meta:\n      classification: Public\n"
LINEAGE_SELECT = "select _object_key as object_key, count(distinct _member_sha256) as checksum_count, count(*) as row_count"
LINEAGE_FROM = "from {{ source('bronze', 'raw_owner') }}\ngroup by _object_key\n"

BRONZE: dict[str, Any] = {
    "version": 1,
    "namespace": "bronze",
    "classification": {
        "collections": {
            "pub/coll": {"class": "Public", "reason": "A public file."},
            "own/coll": {"class": "Confidential", "reason": "Names owners.", "exception": "Personal original kept locally"},
        },
        "loader_tables": {"stored_copies": {"class": "Public", "reason": "File names and checksums."}},
    },
    "tables": [{"table": "raw_public", "collection": "pub/coll"}, {"table": "raw_owner", "collection": "own/coll"}],
}
CONTRACT_DOCUMENT: dict[str, Any] = {
    "facts": {
        "fct_demo": {
            "columns": [
                {"name": "member_sha256", "type": "VARCHAR"},
                {"name": "source_row_number", "type": "BIGINT"},
                {"name": "data_year", "type": "INTEGER"},
                {"name": "amount", "type": "DOUBLE"},
                {"name": "rate", "type": "DOUBLE"},
                {"name": "label", "type": "VARCHAR"},
            ]
        },
        "fct_plain": {"columns": [{"name": "member_sha256", "type": "VARCHAR"}, {"name": "source_row_number", "type": "BIGINT"}]},
    }
}


def document(base: dict[str, Any], change: Callable[[dict[str, Any]], object]) -> str:
    changed = copy.deepcopy(base)
    change(changed)
    return json.dumps(changed, indent=2) + "\n"


BASE = {
    PROJECT: "name: sample\nversion: '1.0.0'\nconfig-version: 2\nvars:\n  copy_tables:\n    raw_public: a\n    raw_owner: b\n",
    MACROS: (
        "{% macro read_staged(table) %}\nselect * from {{ ref('stg_' ~ table) }}\n{% endmacro %}\n\n\n"
        "{% macro gold_fact(silver_model, grain, surrogate_keys) %}\nselect silver.*"
        "{% for key_name, mapping in surrogate_keys.items() %}, {{ key_name }}_dim.owner_name as {{ key_name }}_owner{% endfor %}"
        "\nfrom {{ ref(silver_model) }} as silver\n"
        "{%- for key_name, mapping in surrogate_keys.items() %}\nleft join {{ ref(mapping.dimension) }} as {{ key_name }}_dim on true\n"
        "{%- endfor %}\n{% endmacro %}\n"
    ),
    SOURCES: (
        "version: 2\nsources:\n  - name: bronze\n    tables:\n"
        "      - name: raw_public\n        description: A public table.\n"
        "        columns:\n          - name: state\n          - name: id\n          - name: _object_key\n          - name: _member_sha256\n"
        "        meta:\n          classification: Public\n"
        "      - name: raw_owner\n        description: A table with personal names.\n"
        "        columns:\n          - name: state\n          - name: id\n          - name: _object_key\n          - name: _member_sha256\n"
        "        meta:\n          classification: Confidential\n"
        "          exception: Personal original kept locally\n          confidential_columns: [owner_name]\n"
        "      - name: stored_copies\n        description: Stored file names.\n        meta:\n          classification: Public\n"
    ),
    "dbt/models/silver/staging/stg_raw_public.sql": "select * from {{ source('bronze', 'raw_public') }}\n",
    "dbt/models/silver/staging/stg_raw_owner.sql": "select * from {{ source('bronze', 'raw_owner') }}\n",
    "dbt/models/silver/staging/stg_all_copies.sql": (
        "{% for table in var('copy_tables') %}\nselect * from {{ source('bronze', table) }}\n{% if not loop.last %}union all{% endif %}\n{% endfor %}\n"
    ),
    STAGING: (
        "version: 2\nmodels:\n"
        f"  - name: stg_raw_public\n    description: Staged.\n{PUBLIC}"
        f"  - name: stg_raw_owner\n    description: Staged.\n{CONFIDENTIAL}"
        f"  - name: stg_all_copies\n    description: Staged.\n{CONFIDENTIAL}"
    ),
    "dbt/models/silver/intermediate/int_public.sql": "{{ read_staged('raw_public') }}\n",
    "dbt/models/silver/intermediate/int_owner_by_macro.sql": "{{ read_staged('raw_owner') }}\n",
    INT_MIXED: "select * from {{ ref('int_public') }}\nunion all\nselect * from {{ ref('stg_raw_owner') }}\n",
    "dbt/models/silver/intermediate/int_high_on_public.sql": "select * from {{ ref('int_public') }}\n",
    INTERMEDIATE: (
        "version: 2\nmodels:\n"
        f"  - name: int_public\n    description: Intermediate.\n{PUBLIC}"
        f"  - name: int_owner_by_macro\n    description: Intermediate.\n{CONFIDENTIAL}"
        f"  - name: int_mixed\n    description: Intermediate.\n{CONFIDENTIAL}"
        f"  - name: int_high_on_public\n    description: A Confidential model may read only Public inputs.\n{CONFIDENTIAL}"
    ),
    "dbt/models/gold/dimensions/dim_county.sql": (
        "{%- set sources = [{'model': 'int_public'}, {'model': 'int_owner_by_macro'}] %}\n"
        "{%- for source in sources %}\nselect * from {{ ref(source.model) }}\n{% if not loop.last %}union all{% endif %}\n{%- endfor %}\n"
    ),
    MEASURES: "{%- set value_kinds = {'fct_demo': {'number': ['amount'], 'text': ['label']}} %}\nselect 1 as measure_sk\n",
    DIMENSIONS: (
        f"version: 2\nmodels:\n  - name: dim_county\n    description: Dimension.\n{CONFIDENTIAL}  - name: dim_measure\n    description: Dimension.\n{PUBLIC}"
    ),
    "dbt/models/gold/facts/fct_demo.sql": ("{{ gold_fact('int_public', 'k', {'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'}}) }}\n"),
    "dbt/models/gold/facts/fct_plain.sql": "{{ gold_fact('int_public', 'k', {}) }}\n",
    FACTS: (
        "version: 2\nmodels:\n"
        "  - name: fct_demo\n    description: A fact.\n    meta:\n      classification: Confidential\n      fact_type: periodic_snapshot\n"
        "    columns:\n      - name: amount\n        meta:\n          additivity: additive\n"
        "      - name: rate\n        meta:\n          additivity: non_additive\n"
        "  - name: fct_plain\n    description: A fact with no numeric column.\n    meta:\n      classification: Public\n      fact_type: transaction\n"
    ),
    CONTRACT: json.dumps(CONTRACT_DOCUMENT, indent=2) + "\n",
    BRONZE_MAP: json.dumps(BRONZE, indent=2) + "\n",
}
ALL_FILES_SCOPE = "all"
STAGED_SCOPE = "staged"
CANNOT_RUN_DETAILS = ("Fix:", "ask the owner", "docs/quality_checks.md#model-metadata")


@dataclass(frozen=True)
class Case:
    name: str
    marker: str = ""
    cause: str = ""
    edits: tuple[tuple[str, str, str], ...] = ()
    files: dict[str, str] = field(default_factory=dict)
    code: int = 1
    scope: str = ALL_FILES_SCOPE
    delete: tuple[str, ...] = ()
    rename: tuple[tuple[str, str], ...] = ()
    located: str = ""


def replacing(path: str, old: str, new: str) -> tuple[tuple[str, str, str], ...]:
    return ((path, old, new),)


def model_entry(name: str, class_name: str) -> str:
    return f"  - name: {name}\n    description: Model.\n    meta:\n      classification: {class_name}\n"


def extra_model(sql_path: str, sql: str, yaml_path: str, name: str, class_name: str) -> dict[str, str]:
    return {sql_path: sql, yaml_path: BASE[yaml_path] + model_entry(name, class_name)}


def lineage_model(select_sql: str = f"{LINEAGE_SELECT}\n{LINEAGE_FROM}", entry: str = LINEAGE_ENTRY) -> dict[str, str]:
    reader = model_entry("int_from_lineage", "Public").replace("Model.", "Reads lineage columns.")
    return {
        LINEAGE_SQL: select_sql,
        STAGING: BASE[STAGING] + entry,
        "dbt/models/silver/intermediate/int_from_lineage.sql": "select * from {{ ref('stg_bronze__file_copies') }}\n",
        INTERMEDIATE: BASE[INTERMEDIATE] + reader,
    }


def second_source(source: str) -> str:
    return f"version: 2\nsources:\n  - name: {source}\n    tables:\n      - name: raw_owner\n        meta:\n          classification: Public\n"


def int_mixed_yaml(class_name: str) -> str:
    return BASE[INTERMEDIATE].replace(
        f"  - name: int_mixed\n    description: Intermediate.\n{CONFIDENTIAL}",
        f"  - name: int_mixed\n    description: Intermediate.\n{class_name_meta(class_name)}",
    )


def class_name_meta(class_name: str) -> str:
    return f"    meta:\n      classification: {class_name}\n"


def mixed_read(sql: str, class_name: str = "Public") -> dict[str, str]:
    return {INT_MIXED: sql, INTERMEDIATE: int_mixed_yaml(class_name)}


def unresolved_case(name: str, sql: str, reason: str) -> Case:
    return Case(name, "[unresolved-reference]", f"cannot be resolved ({reason}", files=mixed_read(sql, "Confidential"))


def review_probes() -> list[Case]:
    """Pair each valid reviewer leak with the same query correctly marked Confidential."""
    queries = {
        "p02": "select u from (select owner_name as n, upper(n) as u from {{ source('bronze', 'raw_owner') }}) t\n",
        "p03": "with t as (select owner_name as n, upper(n) as u from {{ source('bronze', 'raw_owner') }}) select u from t\n",
        "p04": "select owner.first_name as x from {{ source('bronze', 'raw_owner') }}\n",
        "p07": "select to_json(raw_owner) as j from {{ source('bronze', 'raw_owner') }}\n",
        "p24": "select l.x from {{ source('bronze', 'raw_owner') }} as r, lateral (select r.owner_name as x) as l\n",
        "implicit_correlated": "select l.x from {{ source('bronze', 'raw_owner') }} as r, (select r.owner_name as x) as l\n",
        "correlated_local": "select l.x from {{ source('bronze', 'raw_owner') }} r, (select r.owner_name as x from {{ source('bronze', 'raw_public') }} p) l\n",
        "correlated_local_unqualified": (
            "select l.x from {{ source('bronze', 'raw_owner') }} r, (select owner_name as x from {{ source('bronze', 'raw_public') }} p) l\n"
        ),
        "correlated_derived": (
            "select l.x from (select owner_name as x from {{ source('bronze', 'raw_owner') }}) r, (select x from {{ source('bronze', 'raw_public') }} p) l\n"
        ),
    }
    cases = []
    for name, sql in queries.items():
        for class_name in ("Public", "Confidential"):
            files = mixed_read(sql, class_name)
            if name == "p04":
                files[SOURCES] = BASE[SOURCES].replace("confidential_columns: [owner_name]", "confidential_columns: [owner]")
            control = class_name == "Confidential"
            cases.append(
                Case(
                    f"review_{name}_{'control' if control else 'leak'}",
                    "" if control else "[class-inheritance]",
                    "" if control else "bronze.raw_owner.owner" if name == "p04" else "bronze.raw_owner.owner_name",
                    files=files,
                    code=0 if control else 1,
                )
            )
    return cases


def onc_read(table: str, sql: str, class_name: str = "Public", reason: str | None = ALL_COLUMNS_REASON, *, all_columns: bool = True) -> dict[str, str]:
    """Return a synthetic ONC source and model fixture with its proposed columns preserved separately from the active declaration."""
    proposed = "[" + ", ".join(ONC_PROPOSALS[table]) + "]"
    declaration = "all" if all_columns else proposed
    metadata = f"          confidential_columns: {declaration}\n          proposed_confidential_columns: {proposed}\n"
    if reason is not None:
        metadata += f"          confidential_columns_reason: {json.dumps(reason)}\n"
    return {
        **mixed_read(sql, class_name),
        SOURCES: BASE[SOURCES].replace("name: raw_owner", f"name: {table}").replace("          confidential_columns: [owner_name]\n", metadata),
        PROJECT: BASE[PROJECT].replace("raw_owner: b", f"{table}: b"),
        BRONZE_MAP: document(BRONZE, lambda document: document["tables"][1].update(table=table)),
        "dbt/models/silver/staging/stg_raw_owner.sql": BASE["dbt/models/silver/staging/stg_raw_owner.sql"].replace("'raw_owner'", repr(table)),
    }


def literal_child(files: dict[str, str]) -> dict[str, str]:
    return {
        **files,
        "dbt/models/silver/intermediate/int_literal_child.sql": "select label from {{ ref('int_mixed') }}\n",
        INTERMEDIATE: files[INTERMEDIATE] + model_entry("int_literal_child", "Public"),
    }


def scoped_resolution_cases() -> list[Case]:
    """Return cases that distinguish per-reference uncertainty from whole-model propagation."""
    unused = "with ignored as (select owner_name from {{ source('bronze', 'raw_owner') }}) __projection__"
    onc = "select product_name as payload, 'fixed' as label from {{ source('bronze', 'onc_pi_chpl_linkage_csv') }}\n"
    ambiguous = (
        "select unpublished_metric as payload, 'fixed' as label from {{ source('bronze', 'raw_public') }} p "
        "join {{ source('bronze', 'raw_owner') }} o on p.id = o.id\n"
    )
    return [
        Case(
            "resolution_public_unknown",
            code=0,
            files=mixed_read(unused.replace("__projection__", "select unpublished_metric from {{ source('bronze', 'raw_public') }}\n")),
        ),
        Case(
            "resolution_public_declared_control",
            code=0,
            files=mixed_read(unused.replace("__projection__", "select state from {{ source('bronze', 'raw_public') }}\n")),
        ),
        Case(
            "resolution_qualified_public_unknown",
            code=0,
            files=mixed_read(unused.replace("__projection__", "select p.unpublished_metric from {{ source('bronze', 'raw_public') }} p\n")),
        ),
        Case(
            "resolution_public_struct",
            code=0,
            files=mixed_read(unused.replace("__projection__", "select details.label from {{ source('bronze', 'raw_public') }}\n")),
        ),
        Case("resolution_cms_unlisted", code=0, files=mixed_read("select unpublished_metric from {{ source('bronze', 'raw_owner') }} r\n")),
        Case("resolution_onc_unknown_leak", "[class-inheritance]", "bronze.onc_pi_chpl_linkage_csv.*", files=onc_read("onc_pi_chpl_linkage_csv", onc)),
        Case("resolution_onc_split_output", code=0, files=literal_child(onc_read("onc_pi_chpl_linkage_csv", onc, "Confidential"))),
        Case("resolution_ambiguous_unknown", "[class-inheritance]", "bronze.raw_owner.owner_name", files=mixed_read(ambiguous)),
        Case("resolution_ambiguous_split_output", code=0, files=literal_child(mixed_read(ambiguous, "Confidential"))),
        Case(
            "resolution_ambiguous_declared_control",
            code=0,
            files=mixed_read(ambiguous.replace("unpublished_metric as payload, 'fixed' as label", "p.state")),
        ),
        Case(
            "resolution_unknown_model_column",
            "[class-inheritance]",
            "bronze.raw_owner.owner_name",
            files=mixed_read("select unpublished_metric from {{ ref('stg_raw_owner') }}\n"),
        ),
    ]


def review_round4_cases() -> list[Case]:
    """Return method, positional, alias-shadow and local-analysis regressions with identical-query controls."""
    queries = {
        "method": "select owner_name.upper() as y from {{ source('bronze', 'raw_owner') }}\n",
        "method_qualified": "select r.owner_name.upper() as y from {{ source('bronze', 'raw_owner') }} r\n",
        "method_cte": "with a as (select owner_name.upper() as u from {{ source('bronze', 'raw_owner') }}) select u from a\n",
        "method_derived": "select * from (select owner_name.upper() as u from {{ source('bronze', 'raw_owner') }}) t\n",
        "method_onc": "select product_name.upper() as y from {{ source('bronze', 'onc_pi_chpl_linkage_csv') }}\n",
        "positional": "select #1 from (select owner_name, state from {{ source('bronze', 'raw_owner') }}) t\n",
        "alias_onc": "select 'x' as product_name, upper(product_name) as y from {{ source('bronze', 'onc_pi_chpl_linkage_csv') }}\n",
        "alias_model": "select 'x' as owner_name, upper(owner_name) as y from {{ ref('stg_all_copies') }}\n",
        "alias_multiple": (
            "select 'x' as product_name, upper(product_name) as y from {{ source('bronze', 'onc_pi_chpl_linkage_csv') }} o "
            "join {{ source('bronze', 'raw_public') }} p on true\n"
        ),
        "analysis_json_arrow": "select to_json(owner_name)->'$' as y from {{ source('bronze', 'raw_owner') }}\n",
        "analysis_pivot": "pivot {{ source('bronze', 'raw_owner') }} on state using first(owner_name)\n",
    }
    cases = []
    for name, sql in queries.items():
        for class_name in ("Public", "Confidential"):
            control = class_name == "Confidential"
            onc = name in {"method_onc", "alias_onc", "alias_multiple"}
            files = onc_read("onc_pi_chpl_linkage_csv", sql, class_name) if onc else mixed_read(sql, class_name)
            cases.append(
                Case(
                    f"review4_{name}_{'control' if control else 'leak'}",
                    "" if control else "[class-inheritance]",
                    "" if control else "bronze.onc_pi_chpl_linkage_csv.*" if onc else "bronze.raw_owner.owner_name",
                    files=files,
                    code=0 if control else 1,
                )
            )
    public_alias = (
        "with ignored as (select owner_name from {{ source('bronze', 'raw_owner') }}) "
        "select 'x' as unpublished_metric, upper(unpublished_metric) as y from {{ source('bronze', 'raw_public') }}\n"
    )
    cases.append(Case("review4_alias_public_control", code=0, files=mixed_read(public_alias)))
    files = mixed_read(queries["analysis_json_arrow"], "Confidential")
    files[INTERMEDIATE] = files[INTERMEDIATE].replace(
        f"  - name: int_owner_by_macro\n    description: Intermediate.\n{CONFIDENTIAL}",
        f"  - name: int_owner_by_macro\n    description: Intermediate.\n{PUBLIC}",
    )
    cases.append(Case("review4_analysis_continues", "[class-inheritance]", "int_owner_by_macro is Public", files=files))
    return cases


def review_round5_cases() -> list[Case]:
    """Return FROM-table-function leak pairs, an existing range control and a conservative diagnostic pair."""
    source = "{{ source('bronze', 'raw_owner') }}"
    queries = {
        "from_qualified_receiver": "select u from __source__ r, unnest([r.owner_name]) as t(u)\n",
        "from_unqualified_receiver": "select u from __source__, unnest([owner_name]) as t(u)\n",
        "from_qualified_output": "select t.u from __source__, unnest([owner_name]) as t(u)\n",
        "from_bare_output": "select unnest from __source__, unnest([owner_name])\n",
        "from_cross_join": "select u from __source__ cross join unnest([owner_name]) as t(u)\n",
        "from_string_split": "select u from __source__, unnest(string_split(owner_name, 'z')) as t(u)\n",
        "range": "select substr(owner_name, x, 1) as y from __source__, range(1, 2) as t(x)\n",
    }
    cases = []
    for name, sql in queries.items():
        for class_name in ("Public", "Confidential"):
            control = class_name == "Confidential"
            cases.append(
                Case(
                    f"review5_{name}_{'control' if control else 'leak'}",
                    "" if control else "[class-inheritance]",
                    "" if control else "bronze.raw_owner.owner_name",
                    files=mixed_read(sql.replace("__source__", source), class_name),
                    code=0 if control else 1,
                )
            )
    sql = "select to_json(state)->'$' as y from {{ source('bronze', 'raw_public') }}\n"
    cases.extend(
        [
            Case("review5_analysis_reason", "[class-inheritance]", "conservative: SQL analysis failed", files=mixed_read(sql)),
            Case("review5_analysis_reason_control", code=0, files=mixed_read(sql, "Confidential")),
        ]
    )
    return cases


def owner_followup_cases() -> list[Case]:
    """Exercise the publisher-only rule, cost-report notes and predicate-only correlation."""
    cases = []
    note = "sum across hospitals only after choosing one report and period per hospital"
    for table in ONC_PROPOSALS:
        for route, sql in {
            "model": "select _object_key, _member_sha256 from {{ ref('stg_raw_owner') }}\n",
            "cte": "with s as (select * from {{ source('bronze', '__table__') }}) select _object_key, _member_sha256 from s\n",
        }.items():
            cases.append(Case(f"owner84_{table}_public_lineage_{route}", code=0, files=onc_read(table, sql.replace("__table__", table))))
        for class_name in ("Public", "Confidential"):
            cases.append(
                Case(
                    f"owner84_{table}_{class_name.lower()}_lineage",
                    code=0,
                    files=onc_read(
                        table,
                        "select _object_key as object_key, count(distinct _member_sha256) as checksum_count "
                        "from {{ source('bronze', '__table__') }} group by _object_key\n".replace("__table__", table),
                        class_name,
                    ),
                )
            )
            control = class_name == "Confidential"
            cases.append(
                Case(
                    f"owner84_{table}_{class_name.lower()}_declared_publisher",
                    "" if control else "[class-inheritance]",
                    "" if control else f"bronze.{table}",
                    code=0 if control else 1,
                    files=onc_read(table, "select state from {{ source('bronze', '__table__') }}\n".replace("__table__", table), class_name),
                )
            )
            cases.append(
                Case(
                    f"owner84_{table}_{class_name.lower()}_publisher_alias",
                    "" if control else "[class-inheritance]",
                    "" if control else f"bronze.{table}",
                    code=0 if control else 1,
                    files=onc_read(
                        table, "select product_name as _object_key from {{ source('bronze', '__table__') }}\n".replace("__table__", table), class_name
                    ),
                )
            )
    cost_files = {path.replace("fct_demo", "fct_cost_report"): text.replace("fct_demo", "fct_cost_report") for path, text in BASE.items()}
    cost_files[FACTS] = cost_files[FACTS].replace("additivity: additive\n", "additivity: semi_additive\n          additive_across: [hospital]\n")
    for suffix, restriction in (("missing", ""), ("wrong", "sum across hospitals"), ("control", note)):
        files = dict(cost_files)
        if restriction:
            files[FACTS] = files[FACTS].replace("additive_across: [hospital]\n", f"additive_across: [hospital]\n          additivity_note: {restriction}\n")
        control = suffix == "control"
        cases.append(
            Case(
                "owner87_cost_note_" + suffix,
                "" if control else "[additivity-note]",
                "" if control else "fct_cost_report.amount",
                files=files,
                delete=("dbt/models/gold/facts/fct_demo.sql",),
                code=0 if control else 1,
            )
        )
    queries = {
        "where": "select r.state from {{ source('bronze', 'raw_owner') }} r where exists (select 1 where r.owner_name is not null)",
        "having": (
            "select r.state from {{ source('bronze', 'raw_owner') }} r group by r.state "
            "having exists (select 1 from {{ source('bronze', 'raw_owner') }} s where s.state = r.state)"
        ),
        "join_on": (
            "select r.state from {{ source('bronze', 'raw_owner') }} r join {{ source('bronze', 'raw_public') }} p "
            "on exists (select 1 where r.owner_name is not null)"
        ),
        "where_unqualified": (
            "select r.state from {{ source('bronze', 'raw_owner') }} r where exists "
            "(select 1 from {{ source('bronze', 'raw_public') }} p where owner_name is not null)"
        ),
        "select_list": "select (select r.owner_name) as x from {{ source('bronze', 'raw_owner') }} r",
        "source_predicate": (
            "select l.state from {{ source('bronze', 'raw_owner') }} r, "
            "(select p.state from {{ source('bronze', 'raw_public') }} p where r.owner_name is not null) l"
        ),
    }
    for name, sql in queries.items():
        for class_name in ("Public", "Confidential"):
            leak = name in {"select_list", "source_predicate"} and class_name == "Public"
            cases.append(
                Case(
                    f"owner94_{name}_{class_name.lower()}",
                    "[class-inheritance]" if leak else "",
                    "bronze.raw_owner.owner_name" if leak else "",
                    code=1 if leak else 0,
                    files=mixed_read(sql + "\n", class_name),
                )
            )
    cms_sql = "select role_text_owner from {{ ref('stg_raw_owner') }}\n"
    for class_name in ("Public", "Confidential"):
        control = class_name == "Confidential"
        cases.append(
            Case(
                f"owner126_cms_undeclared_staging_{class_name.lower()}",
                "" if control else "[class-inheritance]",
                "" if control else "bronze.raw_owner.owner_name",
                files=mixed_read(cms_sql, class_name),
                code=0 if control else 1,
            )
        )
    declared = mixed_read(cms_sql)
    declared[SOURCES] = BASE[SOURCES].replace(
        "          - name: _member_sha256\n        meta:\n          classification: Confidential",
        "          - name: _member_sha256\n          - name: role_text_owner\n        meta:\n          classification: Confidential",
    )
    cases.append(Case("owner126_cms_declared_staging_public", code=0, files=declared))
    return cases


def review_round7_cases() -> list[Case]:
    """Pair CTE-body correlation leaks with controls and retain the local EXISTS conservative boundary."""
    queries = {
        "p01": "select l.z from __owner__ r, (with c as (select r.owner_name as z from __public__ p) select z from c) l",
        "p02": "select l.z from __owner__ r join (with c as (select r.owner_name as z from __public__ p) select z from c) l on true",
        "p03": "select l.z from __owner__ r, (with c as (select owner_name as z from __public__ p) select z from c) l",
        "p04": "select l.z from __owner__ r, (with c as (select upper(r.owner_name) as z from __public__ p) select z from c) l",
        "p05": "select l.z from __owner__ r, (select d.z from (with c as (select r.owner_name as z from __public__ p) select z from c) d) l",
        "p06": "select l.z from __owner__ r, (with c as (select r.owner_name as w from __public__ p), d as (select w as z from c) select z from d) l",
        "p07": "select l.z from __owner__ as outer_t, (with c as (select outer_t.owner_name as z from __public__ p) select z from c) l",
        "p08": "select l.z from {{ ref('stg_raw_owner') }} r, (with c as (select r.owner_name as z from __public__ p) select z from c) l",
        "e03": "select l.state from __owner__ r, (with c as (select p.state from __public__ p where r.owner_name is not null) select state from c) l",
        "nested_cte": ("select l.z from __owner__ r, (with c as (with d as (select r.owner_name as z from __public__ p) select z from d) select z from c) l"),
        "nested_cte_filter": (
            "select l.state from __owner__ r, (with c as (with d as (select p.state from __public__ p where r.owner_name is not null) "
            "select state from d) select state from c) l"
        ),
        "c03": "select l.state from __owner__ r, (select p.state from __public__ p where exists (select 1 where p.id is not null)) l",
        "c04": "select l.state from __public__ r, (select p.state from __public__ p where exists (select 1 where p.id is not null)) l",
    }
    cases = []
    for name, sql in queries.items():
        sql = sql.replace("__owner__", "{{ source('bronze', 'raw_owner') }}").replace("__public__", "{{ source('bronze', 'raw_public') }}") + "\n"
        for class_name in ("Public", "Confidential"):
            block = class_name == "Public" and name != "c04"
            cases.append(
                Case(
                    f"review7_{name}_{class_name.lower()}",
                    "[class-inheritance]" if block else "",
                    "bronze.raw_owner.owner_name" if block else "",
                    files=mixed_read(sql, class_name),
                    code=1 if block else 0,
                )
            )
    return cases


def review_round8_cases() -> list[Case]:
    """Pair bare outer-row leaks with controls that preserve local names and filter-only correlation."""
    queries = {
        "p01": "select l.z from __owner__ r, (select r as z from __public__ p) l",
        "p02": "select l.z from __owner__ r, (select to_json(r) as z from __public__ p) l",
        "p03": "select l.z from __owner__ r, (with c as (select to_json(r) as z from __public__ p) select z from c) l",
        "p04": "select l.z from __owner__ r, (with c as (select r as z from __public__ p) select z from c) l",
        "p05": "select l.z from __owner__, (select raw_owner as z from __public__ p) l",
        "p06": "select l.z from (select owner_name from __owner__) r, (select r as z from __public__ p) l",
        "p07": "select l.z from __owner__ r, (select p.state, r as z from __public__ p) l",
        "p08": "select l.z from __owner__ r, (select coalesce(r, null) as z from __public__ p) l",
        "local_column": "select l.z from __owner__ r, (select r as z from __public__ p) l",
        "earlier_alias": "select l.z from __owner__ r, (select 'fixed' as r, r as z from __public__ p) l",
        "cte_shadow": "select l.z from __owner__ r, (with r as (select p.state as z from __public__ p) select z from r) l",
        "filter_only": "select r.state from __owner__ r where exists (select 1 from __public__ p where r is not null)",
    }
    cases = []
    for name, sql in queries.items():
        sql = sql.replace("__owner__", "{{ source('bronze', 'raw_owner') }}").replace("__public__", "{{ source('bronze', 'raw_public') }}") + "\n"
        for class_name in ("Public", "Confidential"):
            block = class_name == "Public" and name.startswith("p")
            files = mixed_read(sql, class_name)
            if name == "local_column":
                files[SOURCES] = BASE[SOURCES].replace("          - name: state\n", "          - name: state\n          - name: r\n", 1)
            cases.append(
                Case(
                    f"review8_{name}_{class_name.lower()}",
                    "[class-inheritance]" if block else "",
                    "bronze.raw_owner.owner_name" if block else "",
                    files=files,
                    code=1 if block else 0,
                )
            )
    return cases


def make_cases() -> list[Case]:
    """Return independent synthetic cases with the expected hook result and its required diagnostic."""
    unlisted = "dbt/models/silver/staging/stg_unlisted.sql"
    owner_public = replacing(
        STAGING, f"  - name: stg_raw_owner\n    description: Staged.\n{CONFIDENTIAL}", f"  - name: stg_raw_owner\n    description: Staged.\n{PUBLIC}"
    )
    return [
        Case("control", code=0),
        *review_round8_cases(),
        *review_round7_cases(),
        *owner_followup_cases(),
        *review_round5_cases(),
        *review_round4_cases(),
        *scoped_resolution_cases(),
        Case(
            "known_seed_conservative_control",
            code=0,
            files={**mixed_read("select label from {{ ref('public_lookup') }}\n"), "dbt/seeds/public_lookup.csv": "label\nfixed\n"},
        ),
        *review_probes(),
        Case(
            "obsolete_override_rejected",
            "[lineage-override]",
            "classification_override is obsolete",
            files=lineage_model(
                entry=LINEAGE_ENTRY
                + "      classification_override:\n        reason: 'lineage columns only: object keys and file hashes, no row content'\n"
                + "        lineage_columns: [_object_key, _member_sha256]\n"
            ),
        ),
        Case(
            "onc_all_attestations_unlisted",
            "[class-inheritance]",
            "input bronze.onc_pi_attestations_csv is Confidential",
            files=onc_read("onc_pi_attestations_csv", "select attestation_year from {{ source('bronze', 'onc_pi_attestations_csv') }}\n"),
        ),
        Case(
            "onc_all_chpl_unlisted_derived",
            "[class-inheritance]",
            "input bronze.onc_pi_chpl_linkage_csv is Confidential",
            files=onc_read(
                "onc_pi_chpl_linkage_csv",
                "with x as (select product_name as renamed from {{ source('bronze', 'onc_pi_chpl_linkage_csv') }}) select upper(renamed) as label from x\n",
            ),
        ),
        Case(
            "onc_all_exclude_candidates",
            "[class-inheritance]",
            "input bronze.onc_pi_chpl_linkage_csv is Confidential",
            files=onc_read(
                "onc_pi_chpl_linkage_csv",
                "select * exclude (facility_id, facility_name, address, city_town, zip_code, telephone_number, developer_name) "
                "from {{ source('bronze', 'onc_pi_chpl_linkage_csv') }}\n",
            ),
        ),
        Case(
            "onc_all_confidential_control",
            code=0,
            files=onc_read("onc_pi_chpl_linkage_csv", "select product_name from {{ source('bronze', 'onc_pi_chpl_linkage_csv') }}\n", "Confidential"),
        ),
        Case(
            "onc_all_literal_control",
            code=0,
            files=onc_read("onc_pi_chpl_linkage_csv", "select 'fixed' as label from {{ source('bronze', 'onc_pi_chpl_linkage_csv') }}\n"),
        ),
        Case(
            "onc_all_missing_reason",
            "[confidential-columns]",
            "all needs a nonempty meta.confidential_columns_reason",
            files=onc_read("onc_pi_attestations_csv", "select 1\n", "Confidential", reason=None),
        ),
        Case(
            "onc_all_blank_reason",
            "[confidential-columns]",
            "all needs a nonempty meta.confidential_columns_reason",
            files=onc_read("onc_pi_chpl_linkage_csv", "select 1\n", "Confidential", reason=" "),
        ),
        Case(
            "compiled_relation_fallback",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read(
                'select state from {{ source(\'bronze\', \'raw_public\') }} union all by name select owner_name from "memory"."bronze"."raw_owner"\n'
            ),
        ),
        Case(
            "unregistered_relation_fallback",
            "[unresolved-reference]",
            "unresolved relation",
            files=mixed_read(
                'select state from {{ source(\'bronze\', \'raw_public\') }} union all by name select owner_name from "memory"."bronze"."unregistered"\n'
            ),
        ),
        Case(
            "content_cte_column_list",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read("with x(renamed) as (select owner_name from {{ source('bronze', 'raw_owner') }}) select renamed from x\n"),
        ),
        Case(
            "content_table_column_list",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read("select renamed from {{ source('bronze', 'raw_owner') }} as x(renamed)\n"),
        ),
        Case("content_whole_row", "[class-inheritance]", "owner_name", files=mixed_read("select x as payload from {{ source('bronze', 'raw_owner') }} as x\n")),
        Case(
            "content_struct_field",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read(
                "with x as (select struct_pack(nested_name := owner_name) as payload "
                "from {{ source('bronze', 'raw_owner') }}) select x.payload.nested_name from x\n"
            ),
        ),
        Case(
            "content_columns_expression",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read("select columns('.*') from {{ source('bronze', 'raw_owner') }}\n"),
        ),
        Case(
            "content_union_by_name",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read(
                "with x as (select state from {{ source('bronze', 'raw_public') }} union all by name "
                "select owner_name from {{ source('bronze', 'raw_owner') }}) select owner_name from x\n"
            ),
        ),
        Case(
            "fallback_after_clean_projection",
            code=0,
            files={
                **mixed_read("select (select 1) as n from {{ ref('int_public') }}\n"),
                "dbt/models/silver/intermediate/int_public.sql": "select state from {{ source('bronze', 'raw_owner') }}\n",
            },
        ),
        Case(
            "fallback_personal_projection",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read("select (select owner_name from {{ source('bronze', 'raw_owner') }}) as label\n"),
        ),
        Case(
            "content_quoted",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read('select "owner_name" as "PublishedName" from {{ source("bronze", "raw_owner") }}\n'),
        ),
        Case(
            "content_unaliased_expression",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read("with x as (select upper(owner_name) from {{ source('bronze', 'raw_owner') }}) select * from x\n"),
        ),
        Case(
            "content_union_position",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read(
                "select state as clean_name from {{ source('bronze', 'raw_public') }} union all select owner_name from {{ source('bronze', 'raw_owner') }}\n"
            ),
        ),
        Case("content_star_excluded", code=0, files=mixed_read("select * exclude (owner_name) from {{ source('bronze', 'raw_owner') }}\n")),
        Case("content_state_only", code=0, files=mixed_read("select state as state_code from {{ source('bronze', 'raw_owner') }}\n")),
        Case(
            "content_renamed",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read("select owner_name as published_label from {{ source('bronze', 'raw_owner') }}\n"),
        ),
        Case(
            "content_derived",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read("select md5(owner_name) as digest from {{ source('bronze', 'raw_owner') }}\n"),
        ),
        Case(
            "content_cte",
            "[class-inheritance]",
            "owner_name",
            files=mixed_read("with x as (select owner_name as renamed from {{ source('bronze', 'raw_owner') }}) select upper(renamed) as label from x\n"),
        ),
        Case(
            "content_join_clean",
            code=0,
            files=mixed_read("select p.state from {{ source('bronze', 'raw_public') }} p left join {{ source('bronze', 'raw_owner') }} o on p.id = o.id\n"),
        ),
        Case(
            "builtins_ref_bypass",
            "[class-inheritance]",
            "input stg_raw_owner is Confidential",
            files=mixed_read("select * from {{ builtins.ref('stg_raw_owner') }}\n"),
        ),
        Case(
            "builtins_source_bypass",
            "[class-inheritance]",
            "input bronze.raw_owner is Confidential",
            files=mixed_read("select * from {{ builtins.source('bronze', 'raw_owner') }}\n"),
        ),
        Case(
            "callable_alias_bypass",
            "[unresolved-reference]",
            "resolve_parent",
            files=mixed_read("{% set resolve_parent = ref %}\nselect * from {{ resolve_parent('stg_raw_owner') }}\n"),
        ),
        Case("context_callable_bypass", "[unresolved-reference]", "context", files=mixed_read("select * from {{ context['ref']('stg_raw_owner') }}\n")),
        Case(
            "same_line_payload_bypass",
            "[class-inheritance]",
            "owner_name",
            files=lineage_model(
                "select _object_key from {{ source('bronze', 'raw_owner') }} union all select owner_name from {{ source('bronze', 'raw_owner') }}\n"
            ),
        ),
        Case(
            "multiline_payload_control",
            "[class-inheritance]",
            "owner_name",
            files=lineage_model(
                "select _object_key from {{ source('bronze', 'raw_owner') }}\nunion all\nselect owner_name from {{ source('bronze', 'raw_owner') }}\n"
            ),
        ),
        Case(
            "source_missing_columns",
            "[confidential-columns]",
            "needs meta.confidential_columns",
            edits=replacing(SOURCES, "          confidential_columns: [owner_name]\n", ""),
        ),
        Case(
            "source_empty_columns",
            "[confidential-columns]",
            "needs meta.confidential_columns",
            edits=replacing(SOURCES, "confidential_columns: [owner_name]", "confidential_columns: []"),
        ),
        Case(
            "model_self_declared_columns",
            "[confidential-columns]",
            "source-only",
            files={
                INT_MIXED: "select * from {{ ref('int_public') }}\n",
                INTERMEDIATE: int_mixed_yaml("Public").replace(
                    "      classification: Public\n", "      classification: Public\n      confidential_columns: [state]\n"
                ),
            },
        ),
        Case(
            "source_missing_class",
            "[source-class]",
            "source table bronze.raw_public has no meta.classification",
            replacing(SOURCES, "        meta:\n          classification: Public\n      - name: raw_owner", "      - name: raw_owner"),
        ),
        Case(
            "source_bad_class",
            "[allowed-values]",
            "bronze.raw_public: classification 'public' is not one of Public, Confidential",
            replacing(SOURCES, "classification: Public\n      - name: raw_owner", "classification: public\n      - name: raw_owner"),
        ),
        Case("model_missing_class", "[model-class]", "model stg_raw_public has no meta.classification", replacing(STAGING, f"Staged.\n{PUBLIC}", "Staged.\n")),
        Case("model_no_yaml_entry", "[model-class]", "model stg_unlisted has no YAML entry", files={unlisted: "select 1\n"}),
        Case("model_lower_than_source", "[class-inheritance]", "stg_raw_owner is Public but its input bronze.raw_owner is Confidential", owner_public),
        Case(
            "model_lower_than_model",
            "[class-inheritance]",
            "int_mixed is Public but its input stg_raw_owner is Confidential",
            replacing(
                INTERMEDIATE,
                f"  - name: int_mixed\n    description: Intermediate.\n{CONFIDENTIAL}",
                f"  - name: int_mixed\n    description: Intermediate.\n{PUBLIC}",
            ),
        ),
        Case(
            "model_lower_via_macro_argument",
            "[class-inheritance]",
            "int_owner_by_macro is Public but its input stg_raw_owner is Confidential",
            replacing(
                INTERMEDIATE,
                f"  - name: int_owner_by_macro\n    description: Intermediate.\n{CONFIDENTIAL}",
                f"  - name: int_owner_by_macro\n    description: Intermediate.\n{PUBLIC}",
            ),
        ),
        Case(
            "fact_lower_than_dimension",
            "[class-inheritance]",
            "fct_demo is Public but its input dim_county is Confidential",
            replacing(
                FACTS,
                "meta:\n      classification: Confidential\n      fact_type: periodic_snapshot",
                "meta:\n      classification: Public\n      fact_type: periodic_snapshot",
            ),
        ),
        Case(
            "model_lower_via_model_list",
            "[class-inheritance]",
            "dim_county is Public but its input int_owner_by_macro is Confidential",
            replacing(
                DIMENSIONS,
                f"  - name: dim_county\n    description: Dimension.\n{CONFIDENTIAL}",
                f"  - name: dim_county\n    description: Dimension.\n{PUBLIC}",
            ),
        ),
        Case(
            "model_lower_via_variable",
            "[class-inheritance]",
            "stg_all_copies is Public but its input bronze.raw_owner is Confidential",
            replacing(
                STAGING,
                f"  - name: stg_all_copies\n    description: Staged.\n{CONFIDENTIAL}",
                f"  - name: stg_all_copies\n    description: Staged.\n{PUBLIC}",
            ),
        ),
        Case("fact_missing_type", "[fact-type]", "gold fact fct_demo has no meta.fact_type", replacing(FACTS, "      fact_type: periodic_snapshot\n", "")),
        Case(
            "fact_bad_type",
            "[allowed-values]",
            "fct_demo: fact_type 'snapshot' is not one of transaction, periodic_snapshot, accumulating_snapshot",
            replacing(FACTS, "fact_type: periodic_snapshot", "fact_type: snapshot"),
        ),
        Case(
            "measure_missing_additivity",
            "[additivity]",
            "measure column fct_demo.amount has no meta.additivity",
            replacing(
                FACTS, "      - name: amount\n        meta:\n          additivity: additive\n", "      - name: amount\n        description: The amount.\n"
            ),
        ),
        Case(
            "measure_missing_column_entry",
            "[additivity]",
            "measure column fct_demo.amount has no meta.additivity",
            replacing(FACTS, "      - name: amount\n        meta:\n          additivity: additive\n", ""),
        ),
        Case(
            "measure_bad_additivity",
            "[allowed-values]",
            "fct_demo.amount: additivity 'summable' is not one of additive, semi_additive, non_additive",
            replacing(FACTS, "additivity: additive", "additivity: summable"),
        ),
        Case(
            "semi_additive_control",
            code=0,
            edits=replacing(FACTS, "additivity: additive\n", "additivity: semi_additive\n          additive_across: [county]\n"),
        ),
        Case(
            "semi_additive_without_axes",
            "[additive-across]",
            "fct_demo.amount is semi_additive but meta.additive_across",
            replacing(FACTS, "additivity: additive\n", "additivity: semi_additive\n"),
        ),
        Case(
            "semi_additive_empty_axes",
            "[additive-across]",
            "fct_demo.amount is semi_additive but meta.additive_across",
            replacing(FACTS, "additivity: additive\n", "additivity: semi_additive\n          additive_across: []\n"),
        ),
        Case(
            "semi_additive_axes_not_a_list",
            "[additive-across]",
            "fct_demo.amount is semi_additive but meta.additive_across",
            replacing(FACTS, "additivity: additive\n", "additivity: semi_additive\n          additive_across: county\n"),
        ),
        Case(
            "semi_additive_blank_axis",
            "[additive-across]",
            "fct_demo.amount is semi_additive but meta.additive_across",
            replacing(FACTS, "additivity: additive\n", "additivity: semi_additive\n          additive_across: [county, '']\n"),
        ),
        Case(
            "measure_omitted_from_dim_measure",
            "[additivity]",
            "measure column fct_demo.rate has no meta.additivity",
            replacing(FACTS, "      - name: rate\n        meta:\n          additivity: non_additive\n", ""),
        ),
        Case(
            "measure_map_emptied",
            "[additivity]",
            "measure column fct_demo.amount has no meta.additivity",
            replacing(FACTS, "      - name: amount\n        meta:\n          additivity: additive\n", "")
            + replacing(MEASURES, "{'fct_demo': {'number': ['amount'], 'text': ['label']}}", "{}"),
        ),
        Case(
            "measure_new_contract_column",
            "[additivity]",
            "measure column fct_demo.bed_count has no meta.additivity",
            files={CONTRACT: document(CONTRACT_DOCUMENT, lambda d: d["facts"]["fct_demo"]["columns"].append({"name": "bed_count", "type": "INTEGER"}))},
        ),
        Case(
            "measure_label_on_excluded_column",
            "[allowed-values]",
            "fct_demo.data_year: additivity 'bogus' is not one of additive, semi_additive, non_additive",
            replacing(FACTS, "      - name: rate\n", "      - name: data_year\n        meta:\n          additivity: bogus\n      - name: rate\n"),
        ),
        Case(
            "measure_unknown_type",
            "[fact-contract]",
            "contract column fct_demo.shape has type 'GEOMETRY' that the check does not know",
            files={CONTRACT: document(CONTRACT_DOCUMENT, lambda d: d["facts"]["fct_demo"]["columns"].append({"name": "shape", "type": "GEOMETRY"}))},
        ),
        Case(
            "fact_not_in_contract",
            "[fact-contract]",
            "gold fact fct_new is not in data_contracts/lakehouse/gold_contract.json",
            files={
                "dbt/models/gold/facts/fct_new.sql": "{{ gold_fact('int_public', 'k', {}) }}\n",
                FACTS: BASE[FACTS] + "  - name: fct_new\n    description: A new fact.\n    meta:\n      classification: Public\n      fact_type: transaction\n",
            },
        ),
        Case(
            "contract_fact_without_model",
            "[fact-contract]",
            "contract fact fct_plain has no model",
            replacing(
                FACTS,
                "  - name: fct_plain\n    description: A fact with no numeric column.\n    meta:\n      classification: Public\n      fact_type: transaction\n",
                "",
            ),
            delete=("dbt/models/gold/facts/fct_plain.sql",),
        ),
        Case("contract_missing", "cannot run", "gold_contract.json", delete=(CONTRACT,), code=2),
        Case("contract_malformed", "cannot run", "", files={CONTRACT: '{"facts": {\n  "fct_demo": [\n'}, code=2, located=CONTRACT),
        Case("bronze_map_malformed", "cannot run", "", files={BRONZE_MAP: '{"tables": [\n  {"table": \n'}, code=2, located=BRONZE_MAP),
        Case("yaml_malformed", "cannot run", "", replacing(SOURCES, "sources:\n", "sources: [\n"), code=2, located=SOURCES),
        Case(
            "source_entry_without_name",
            "cannot run",
            "name",
            replacing(SOURCES, "      - name: raw_public\n", "      - description: no name\n"),
            code=2,
            located=SOURCES,
        ),
        Case(
            "jinja_malformed_model",
            "cannot run",
            "",
            files={"dbt/models/silver/intermediate/int_public.sql": "{{ ref(\n"},
            code=2,
            located="dbt/models/silver/intermediate/int_public.sql",
        ),
        Case("jinja_malformed_macro", "cannot run", "", replacing(MACROS, "{% endmacro %}\n\n\n", "{% endmacro\n\n\n"), code=2, located=MACROS),
        Case(
            "model_lower_via_jinja_variable",
            "[class-inheritance]",
            "int_mixed is Public but its input stg_raw_owner is Confidential",
            files=mixed_read("{% set parent_model = 'stg_raw_owner' %}\nselect * from {{ ref(parent_model) }}\n"),
        ),
        Case(
            "jinja_variable_control",
            code=0,
            files=mixed_read("{% set parent_model = 'stg_raw_owner' %}\nselect * from {{ ref(parent_model) }}\n", "Confidential"),
        ),
        unresolved_case("reference_unresolved_name", "select * from {{ ref(undefined_name) }}\n", "name 'undefined_name' has no value the check can read"),
        unresolved_case(
            "reference_unresolved_variable", "{% set parent = var('missing_var') %}\nselect * from {{ ref(parent) }}\n", "var 'missing_var' is not set"
        ),
        unresolved_case("reference_missing_model", "select * from {{ ref('stg_typo') }}\n", "no model or seed named 'stg_typo'"),
        Case(
            "reference_missing_model_disabled", code=0, files=mixed_read("{{ config(enabled=false) }}\nselect * from {{ ref('stg_typo') }}\n", "Confidential")
        ),
        Case(
            "reference_missing_model_config_without_disable",
            "[unresolved-reference]",
            "no model or seed named 'stg_typo'",
            files=mixed_read("{{ config(materialized='view') }}\nselect * from {{ ref('stg_typo') }}\n", "Confidential"),
        ),
        unresolved_case("reference_package_unsupported", "select * from {{ ref('other_pkg', 'stg_raw_owner') }}\n", "package 'other_pkg' is not this project"),
        Case(
            "model_lower_via_qualified_ref",
            "[class-inheritance]",
            "int_mixed is Public but its input stg_raw_owner is Confidential",
            files=mixed_read("select * from {{ ref('sample', 'stg_raw_owner') }}\n"),
        ),
        Case(
            "macro_in_subfolder",
            "[class-inheritance]",
            "int_via_subfolder is Public but its input stg_raw_owner is Confidential",
            files={
                "dbt/macros/readers/owner_reader.sql": "{% macro read_owner() %}\nselect * from {{ ref('stg_raw_owner') }}\n{% endmacro %}\n",
                **extra_model("dbt/models/silver/intermediate/int_via_subfolder.sql", "{{ read_owner() }}\n", INTERMEDIATE, "int_via_subfolder", "Public"),
            },
        ),
        Case(
            "macro_qualified_call",
            "[class-inheritance]",
            "int_qualified is Public but its input stg_raw_owner is Confidential",
            files=extra_model(
                "dbt/models/silver/intermediate/int_qualified.sql", "{{ sample.read_staged('raw_owner') }}\n", INTERMEDIATE, "int_qualified", "Public"
            ),
        ),
        unresolved_case("macro_undefined", "{{ no_such_macro('raw_owner') }}\n", "macro 'no_such_macro' is not defined in dbt/macros"),
        unresolved_case(
            "macro_unknown_package", "{{ other_pkg.read_staged('raw_owner') }}\n", "macro 'other_pkg.read_staged' belongs to a package the check cannot read"
        ),
        unresolved_case("jinja_include_rejected", "{% include 'int_public.sql' %}\n", "Jinja statement 'include' is not supported"),
        Case(
            "source_file_yaml_extension",
            "[class-inheritance]",
            "stg_raw_owner is Public but its input bronze.raw_owner is Confidential",
            owner_public,
            rename=((SOURCES, SOURCES.replace(".yml", ".yaml")),),
        ),
        Case(
            "source_reference_undeclared",
            "[unresolved-reference]",
            "cannot be resolved (no source table 'bronze.raw_typo' is declared",
            files={"dbt/models/silver/staging/stg_raw_public.sql": "select * from {{ source('bronze', 'raw_typo') }}\n"},
        ),
        Case(
            "source_namespace_collision",
            "[class-inheritance]",
            "stg_raw_owner is Public but its input bronze.raw_owner is Confidential",
            owner_public,
            files={OTHER_SOURCES: second_source("other")},
        ),
        Case(
            "source_duplicate",
            "[duplicate-entry]",
            "source bronze.raw_owner is defined more than once",
            files={OTHER_SOURCES: second_source("bronze")},
        ),
        Case(
            "model_duplicate_yaml",
            "[duplicate-entry]",
            "model stg_raw_public is defined more than once in YAML",
            files={INTERMEDIATE: BASE[INTERMEDIATE] + model_entry("stg_raw_public", "Public")},
        ),
        Case(
            "model_duplicate_sql",
            "[duplicate-entry]",
            "model int_public has more than one SQL file",
            files={"dbt/models/gold/dimensions/int_public.sql": "select 1\n"},
        ),
        Case("lineage_projection_control", code=0, files=lineage_model()),
        Case(
            "lineage_payload_column",
            "[class-inheritance]",
            "owner_name",
            files=lineage_model(f"{LINEAGE_SELECT}, max(owner_name) as owner_name\n{LINEAGE_FROM}"),
        ),
        Case(
            "lineage_payload_star",
            "[class-inheritance]",
            "owner_name",
            files=lineage_model("select * from {{ source('bronze', 'raw_owner') }}\n"),
        ),
        Case(
            "bronze_collection_class_missing",
            "[bronze-class]",
            "bronze table raw_owner: collection own/coll has no class",
            files={BRONZE_MAP: document(BRONZE, lambda d: d["classification"]["collections"].pop("own/coll"))},
        ),
        Case(
            "bronze_source_below_class",
            "[bronze-class]",
            "source raw_public is Public but its bronze table is Confidential",
            files={BRONZE_MAP: document(BRONZE, lambda d: d["tables"][0].update(collection="own/coll"))},
        ),
        Case(
            "bronze_table_override",
            code=0,
            files={
                BRONZE_MAP: document(
                    BRONZE,
                    lambda d: d["tables"][0].update(collection="own/coll", classification={"class": "Public", "reason": "Dictionary only, no person fields."}),
                )
            },
        ),
        Case(
            "bronze_source_without_table",
            "[bronze-class]",
            "source raw_ghost has no bronze table",
            replacing(
                SOURCES,
                "      - name: stored_copies\n",
                "      - name: raw_ghost\n        meta:\n          classification: Public\n      - name: stored_copies\n",
            ),
        ),
        Case(
            "exception_missing_source",
            "[exception]",
            "source bronze.raw_owner is Confidential but has no meta.exception",
            replacing(SOURCES, "          exception: Personal original kept locally\n", ""),
        ),
        Case(
            "exception_empty_source",
            "[exception]",
            "source bronze.raw_owner is Confidential but has no meta.exception",
            replacing(SOURCES, "exception: Personal original kept locally", "exception: ''"),
        ),
        Case(
            "exception_missing_collection",
            "[exception]",
            "bronze collection own/coll is Confidential but has no exception",
            files={BRONZE_MAP: document(BRONZE, lambda d: d["classification"]["collections"]["own/coll"].pop("exception"))},
        ),
        Case(
            "exception_missing_table_override",
            "[exception]",
            "bronze table raw_public is Confidential but has no exception",
            files={BRONZE_MAP: document(BRONZE, lambda d: d["tables"][0].update(classification={"class": "Confidential", "reason": "Held for review."}))},
        ),
        Case("scope_relevant_change", "model metadata ok", "", replacing(STAGING, "Staged.\n", "Staged again.\n"), code=0, scope=STAGED_SCOPE),
        Case(
            "scope_contract_change",
            "model metadata ok",
            "",
            files={CONTRACT: document(CONTRACT_DOCUMENT, lambda d: d["facts"]["fct_demo"]["columns"].append({"name": "county_fips", "type": "VARCHAR"}))},
            code=0,
            scope=STAGED_SCOPE,
        ),
        Case(
            "scope_bronze_map_change",
            "model metadata ok",
            "",
            files={BRONZE_MAP: document(BRONZE, lambda d: d["tables"][0].update(format="csv"))},
            code=0,
            scope=STAGED_SCOPE,
        ),
        Case("scope_unrelated_change", "Skipped", "", files={"NOTES.txt": "unrelated\n"}, code=0, scope=STAGED_SCOPE),
    ]


def git(repo: Path, *args: str) -> str:
    return run_command("git", args, cwd=repo, check=True).stdout


def write(repo: Path, files: dict[str, str]) -> None:
    for relative, content in files.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)


def scratch_repo(root: Path, checker: Path, hook_config: Path) -> Path:
    repo = root / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.name", "check test")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "config", "gc.auto", "0")
    git(repo, "config", "maintenance.auto", "false")
    (repo / "scripts" / "quality").mkdir(parents=True)
    shutil.copy(hook_config, repo / ".pre-commit-config.yaml")
    shutil.copy(checker, repo / "scripts" / "quality" / "model_metadata.py")
    (repo / ".venv").symlink_to(ROOT / ".venv")
    (repo / ".git" / "info" / "exclude").write_text(".venv\n.tools\n__pycache__/\n")
    shutil.copy(ROOT / "scripts" / "process.py", repo / "scripts" / "process.py")
    (repo / ".tools").symlink_to(ROOT / ".tools")
    write(repo, BASE)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "chore: scratch setup")
    return repo


def apply(repo: Path, case: Case) -> None:
    for relative, old, new in case.edits:
        text = (repo / relative).read_text()
        if old not in text:
            raise AssertionError(f"{case.name}: edit text not found in {relative}: {old!r}")
        (repo / relative).write_text(text.replace(old, new, 1))
    write(repo, case.files)
    for relative in case.delete:
        git(repo, "rm", "-q", relative)
    for old, new in case.rename:
        git(repo, "mv", old, new)
    git(repo, "add", "-A")


def judge(case: Case, exit_code: int, output: str, exact: bool) -> bool:
    expected_exit = case.code if exact else int(case.code != 0)
    correct = (exit_code == expected_exit) if exact else ((exit_code != 0) == (case.code != 0))
    correct = correct and "Traceback (most recent call last)" not in output
    if case.marker:
        correct = correct and case.marker in output and case.cause in output
    if case.code == 2:
        correct = correct and all(detail in output for detail in CANNOT_RUN_DETAILS)
    if case.located:
        correct = correct and re.search(rf"{re.escape(case.located)}:\d+", output) is not None
    if case.name == "control":
        correct = correct and "model metadata ok" in output
    return correct


def run_case(case: Case, checker: Path, hook_config: Path, case_seconds: int = CASE_SECONDS) -> dict[str, Any]:
    """Run a synthetic case through the given hook and checker and return their assertions and diagnostic tails."""
    began = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="hai_model_metadata_") as scratch:
        repo = scratch_repo(Path(scratch), checker, hook_config)
        apply(repo, case)
        args = [
            "run",
            HOOK,
            "--hook-stage",
            hook_stage(repo / ".pre-commit-config.yaml", HOOK),
            "--verbose",
            *(["--all-files"] if case.scope == ALL_FILES_SCOPE else []),
        ]
        environment = {**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools" / "pre_commit_cache")}
        hook = run_command(str(PRE_COMMIT), args, cwd=repo, env=environment, timeout=case_seconds)
        hook_output = hook.stdout + hook.stderr
        hook_correct = judge(case, hook.returncode, hook_output, exact=False)
        direct_correct, direct_code, direct_tail = True, None, []
        if case.scope == ALL_FILES_SCOPE:
            direct = run_command(str(repo / ".venv" / "bin" / "python"), ["-m", "scripts.quality.model_metadata"], cwd=repo, timeout=case_seconds)
            direct_code, direct_output = direct.returncode, direct.stdout + direct.stderr
            direct_correct = judge(case, direct.returncode, direct_output, exact=True)
            direct_tail = direct_output.strip().splitlines()[-6:]
        return {
            "name": case.name,
            "expected": "pass" if case.code == 0 else f"block ({case.marker}), exit {case.code}",
            "hook_exit": hook.returncode,
            "direct_exit": direct_code,
            "correct": hook_correct and direct_correct,
            "hook_correct": hook_correct,
            "direct_correct": direct_correct,
            "tail": hook_output.strip().splitlines()[-6:],
            "direct_tail": direct_tail,
            "seconds": round(time.monotonic() - began, 2),
        }


def worker_count(value: str) -> int:
    """The number of cases that run at the same time: ``auto`` is the CPU count, at most MAX_AUTO_WORKERS; a number is taken as given."""
    if value == "auto":
        return max(1, min(MAX_AUTO_WORKERS, os.cpu_count() or 1))
    count = int(value)
    if count < 1:
        raise ValueError(f"--workers must be auto or a number of at least 1, not {value}")
    return count


def outcome(result: dict[str, Any]) -> dict[str, Any]:
    """What a case showed, without the seconds and the hook's own duration line, which differ between runs."""
    keys = ("name", "expected", "hook_exit", "direct_exit", "correct", "hook_correct", "direct_correct")
    tails = {key: [line for line in result[key] if not line.startswith("- duration:")] for key in ("tail", "direct_tail")}
    return {**{key: result[key] for key in keys}, **tails}


def failed_case(case: Case, reason: str) -> dict[str, Any]:
    return {
        "name": case.name,
        "expected": "pass" if case.code == 0 else f"block ({case.marker}), exit {case.code}",
        "hook_exit": None,
        "direct_exit": None,
        "correct": False,
        "hook_correct": False,
        "direct_correct": False,
        "tail": [reason],
        "direct_tail": [],
        "seconds": None,
    }


def run_cases(cases: list[Case], checker: Path, hook_config: Path, workers: int, case_seconds: int = CASE_SECONDS) -> list[dict[str, Any]]:
    """Run the cases and return their results in case order, one after another or with ``workers`` at the same time.

    Each case has its own scratch repository, so parallel cases share only the read-only pre-commit cache. Each case starts at most two programs,
    each with a deadline of ``case_seconds``; the pool as a whole has a deadline of the cases each worker takes times two such programs, plus
    one minute. A case that raises or is unfinished at the deadline is reported as wrong with the reason, so a hang or crash fails visibly.
    """
    if workers == 1:
        results: list[dict[str, Any]] = []
        for case in cases:
            try:
                results.append(run_case(case, checker, hook_config, case_seconds))
            except TimeoutExpired as error:
                results.append(failed_case(case, f"deadline: a program of this case ran longer than {error.timeout} seconds"))
        return results
    deadline = -(-len(cases) // workers) * 2 * case_seconds + 60
    pool = ThreadPoolExecutor(max_workers=workers)
    futures = [pool.submit(run_case, case, checker, hook_config, case_seconds) for case in cases]
    done, unfinished = wait(futures, timeout=deadline)
    pool.shutdown(wait=False, cancel_futures=True)
    results = []
    for case, future in zip(cases, futures, strict=True):
        if future not in done:
            results.append(failed_case(case, f"deadline: not finished {deadline} seconds after the start of the pool ({len(unfinished)} cases unfinished)"))
        elif isinstance(future.exception(), TimeoutExpired):
            results.append(failed_case(case, f"deadline: a program of this case ran longer than {case_seconds} seconds"))
        else:
            results.append(future.result())
    return results


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def exercise_personal_scan(
    directory: Path,
    name: str,
    content: str | bytes,
    suffix: str,
    expected: int,
    kinds: set[str],
    *,
    columns: list[str] | None = None,
    inventory: list[str] | None = None,
    csv_header: str | None = None,
    paths: set[str] | None = None,
    forbidden: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Run a synthetic export twice through the scanner CLI and return findings, disclosure, coverage and repeatability assertions."""
    folder = directory / name
    folder.mkdir()
    data = folder / ("export" + suffix)
    data.write_bytes(content) if isinstance(content, bytes) else data.write_text(content)
    input_hash = hashlib.sha256(data.read_bytes()).hexdigest()
    if name == "symlink":
        target = folder / ("real_export" + suffix)
        data.rename(target)
        data.symlink_to(target)
    manifest = folder / "manifest.json"
    spec: dict[str, Any] = {"collection": "synthetic/scan", "table": "sample", "files": [str(data)]}
    if csv_header is not None:
        header = folder / "schema.csv"
        header.write_text(csv_header)
        spec["files"].insert(0, str(header))
    if columns is not None:
        spec["columns"] = columns
    manifest.write_text(json.dumps({"tables": [spec]}))
    output = data if name == "output_overlaps_input" else folder / "result.json"
    command_args = ["-m", "scripts.quality.model_metadata", "scan-personal-data", "--manifest", str(manifest), "--output", str(output)]
    if inventory is not None:
        inventory_path = folder / "inventory.json"
        inventory_path.write_text(json.dumps({"tables": [{"collection": "synthetic/scan", "table": table} for table in inventory]}))
        command_args.extend(["--inventory", str(inventory_path)])
    result = run_command(sys.executable, command_args, cwd=ROOT, timeout=30)
    observed = json.loads(output.read_text()) if output.exists() else {}
    findings = [item for table in observed.get("tables", []) for item in table.get("findings", [])]
    actual_kinds = {item["kind"] for item in findings}
    emitted = result.stdout + result.stderr + json.dumps(observed)
    leaks = any(
        value.casefold() in emitted.casefold()
        for value in ("Alex Example", "Avery Sample", "123 Example Street", "sample@example.invalid", "2000-01-02", "DO_NOT_ECHO", *forbidden)
    )
    passed = result.returncode == expected and kinds <= actual_kinds and not leaks and "Traceback" not in emitted
    if expected == 0:
        passed = passed and observed.get("tables") is not None and not findings
    if paths is not None:
        passed = passed and {finding["column_path"] for finding in findings} == paths
    if inventory is not None:
        passed = passed and observed.get("complete") == (expected != 2)
    if name == "output_overlaps_input":
        passed = passed and hashlib.sha256(data.read_bytes()).hexdigest() == input_hash
    repeat = run_command(sys.executable, command_args, cwd=ROOT, timeout=30)
    passed = passed and repeat.returncode == result.returncode and (json.loads(output.read_text()) if output.exists() else {}) == observed
    return {
        "case": name,
        "expected_exit": expected,
        "exit": result.returncode,
        "expected_kinds": sorted(kinds),
        "kinds": sorted(actual_kinds),
        "value_leak": leaks,
        "complete": observed.get("complete"),
        "column_paths": sorted({finding["column_path"] for finding in findings}),
        "passed": passed,
    }


def run_personal_scan_e2e(argv: list[str]) -> int:
    """Run synthetic scanner cases, save a repeatable report and return 1 when any assertion fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, default=ROOT / "data/e2e/personal_data_scan")
    args = parser.parse_args(argv)
    started = datetime.now(UTC)
    before = hashlib.sha256(CHECKER.read_bytes()).hexdigest()
    positive = {
        "full_name": "Alex Example",
        "street_address": "123 Example Street",
        "date_of_birth": "2000-01-02",
        "patient_id": "synthetic-0001",
        "notes": "Contact: Avery Sample. Email: sample@example.invalid. Phone: +1 (202) 555-0148. DOB: 1999-12-31",
    }
    with tempfile.TemporaryDirectory(prefix="hai_personal_scan_e2e_") as scratch:
        directory = Path(scratch)
        results = [
            exercise_personal_scan(directory, "generic_name_column", '{"name":"alex example"}\n', ".jsonl", 1, {"name"}),
            exercise_personal_scan(directory, "labeled_lowercase_name", '{"notes":"Name: alex example"}\n', ".jsonl", 1, {"name"}),
            exercise_personal_scan(directory, "schema_only_csv", "full_name,notes\n", ".csv", 1, {"name"}),
            exercise_personal_scan(directory, "symlink", '{"state":"CA"}\n', ".jsonl", 2, set()),
            exercise_personal_scan(directory, "output_overlaps_input", '{"state":"CA"}\n', ".jsonl", 2, set()),
            exercise_personal_scan(directory, "invalid_utf8", b"\xff\xfe\x00", ".txt", 2, set()),
            exercise_personal_scan(directory, "clean_jsonl", '{"state":"CA","beds":250,"notes":"staffing increased in the report."}\n', ".jsonl", 0, set()),
            exercise_personal_scan(
                directory,
                "planted_jsonl",
                json.dumps(positive) + "\n",
                ".jsonl",
                1,
                {"name", "address", "date_of_birth", "identifier", "email", "phone"},
            ),
            exercise_personal_scan(
                directory,
                "planted_text",
                "Prepared by: Alex Example\n123 Example Street\nSSN: 000-00-0000\n",
                ".txt",
                1,
                {"name", "address", "identifier"},
            ),
            exercise_personal_scan(directory, "planted_csv", "full_name,notes\nAlex Example,staffing increased\n", ".csv", 1, {"name"}),
            exercise_personal_scan(
                directory,
                "nested_json",
                json.dumps([{"person": positive, "observations": [positive]}]),
                ".json",
                1,
                {"name", "identifier", "date_of_birth"},
            ),
            exercise_personal_scan(directory, "malformed_json", '{"DO_NOT_ECHO":', ".json", 2, set()),
            exercise_personal_scan(directory, "unsupported_format", "DO_NOT_ECHO", ".bin", 2, set()),
            exercise_personal_scan(directory, "dynamic_column_name", '{"Alex Example":"staffing"}\n', ".jsonl", 1, {"name"}),
            exercise_personal_scan(
                directory,
                "lowercase_dynamic_key",
                '{"alex_example":"Name: alex example"}\n',
                ".jsonl",
                1,
                {"name"},
                paths={"field_0"},
                forbidden=("alex_example",),
            ),
            exercise_personal_scan(
                directory,
                "nested_dynamic_key",
                '{"records":[{"alex_example":"Name: alex example"}]}',
                ".json",
                1,
                {"name"},
                columns=["records"],
                paths={"records[].field_0"},
                forbidden=("alex_example",),
            ),
            exercise_personal_scan(directory, "declared_schema", '{"notes":"Name: alex example"}\n', ".jsonl", 1, {"name"}, columns=["notes"], paths={"notes"}),
            exercise_personal_scan(
                directory, "first_csv_header", '{"notes":"Name: alex example"}\n', ".jsonl", 1, {"name"}, csv_header="notes\n", paths={"notes"}
            ),
            exercise_personal_scan(
                directory,
                "json_keys_not_csv_columns",
                '{"alex_example":"Name: alex example"}\n',
                ".jsonl",
                1,
                {"name"},
                csv_header="notes\n",
                paths={"field_0"},
                forbidden=("alex_example",),
            ),
            exercise_personal_scan(directory, "inventory_complete", '{"state":"CA"}\n', ".jsonl", 0, set(), inventory=["sample"]),
            exercise_personal_scan(directory, "inventory_missing_table", '{"state":"CA"}\n', ".jsonl", 2, set(), inventory=["sample", "absent"]),
            exercise_personal_scan(directory, "invalid_schema", '{"state":"CA"}\n', ".jsonl", 2, set(), columns=["notes", "notes"]),
        ]
    stable = hashlib.sha256(CHECKER.read_bytes()).hexdigest() == before
    report = {
        "started": started.isoformat(),
        "scope": "synthetic local exports only",
        "scanner_sha256": before,
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python": sys.version.split()[0],
        "stable_code": stable,
        "passed": sum(item["passed"] for item in results),
        "total": len(results),
        "cases": results,
        "cleanup": "done",
    }
    output = args.output_directory / started.strftime("%Y%m%dT%H%M%S%fZ")
    output.mkdir(parents=True)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    for item in results:
        sys.stdout.write(f"{'ok' if item['passed'] else 'WRONG'} {item['case']} exit {item['exit']} kinds {item['kinds']}\n")
    sys.stdout.write(f"{report['passed']} of {report['total']} cases; report {output / 'report.json'}\n")
    return int(not stable or report["passed"] != report["total"])


def main() -> int:
    """Run every case, write the report and return 1 when any case did not behave as expected."""
    if len(sys.argv) > 1 and sys.argv[1] == "scan-personal-data":
        return run_personal_scan_e2e(sys.argv[2:])
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--checker", type=Path, default=CHECKER, help="checker file to run the cases against")
    parser.add_argument("--hook-config", type=Path, default=ROOT / ".pre-commit-config.yaml", help="hook configuration to run the cases against")
    parser.add_argument("--output-directory", type=Path, default=ROOT / "data" / "e2e" / "model_metadata")
    parser.add_argument("--case", action="append", default=[], help="run only this case (repeatable)")
    parser.add_argument("--workers", default="1", help="cases that run at the same time: a number, or auto (the CPU count, at most 4); 1 is the serial mode")
    parser.add_argument("--case-seconds", type=int, default=CASE_SECONDS, help="deadline in seconds of each program a case starts")
    parser.add_argument("--prove-parallel-equal", action="store_true", help="run the cases serially and with --workers, and fail unless the outcomes are equal")
    args = parser.parse_args()
    clear_git_environment()
    if not PRE_COMMIT.exists():
        sys.stderr.write("setup missing: create .venv from requirements.txt (README, Install)\n")
        return 1
    cases = [case for case in make_cases() if not args.case or case.name in args.case]
    unknown = sorted(set(args.case) - {case.name for case in cases})
    if unknown:
        sys.stderr.write(f"unknown cases: {', '.join(unknown)}\n")
        return 1
    started = datetime.now(UTC)
    checker_before, hook_before, runner_before = sha256_of(args.checker), sha256_of(args.hook_config), sha256_of(Path(__file__))
    workers = worker_count(args.workers)
    results = run_cases(cases, args.checker, args.hook_config, workers, args.case_seconds)
    if args.prove_parallel_equal:
        serial = run_cases(cases, args.checker, args.hook_config, 1, args.case_seconds)
        different = [a["name"] for a, b in zip(results, serial, strict=True) if outcome(a) != outcome(b)]
        sys.stdout.write(f"serial and {workers} workers: {len(results)} cases, {len(different)} different{': ' + ', '.join(different) if different else ''}\n")
        if different or workers == 1:
            return 1
    scanner_exit = 0 if args.case else run_personal_scan_e2e(["--output-directory", str(args.output_directory / "scanner")])
    stable = checker_before == sha256_of(args.checker) and hook_before == sha256_of(args.hook_config) and runner_before == sha256_of(Path(__file__))
    for result in results:
        label = "ok" if result["correct"] else "WRONG"
        sys.stdout.write(f"{label:<6} {result['name']:<38} expected {result['expected']}, hook {result['hook_exit']}, direct {result['direct_exit']}\n")
        if not result["correct"]:
            sys.stdout.write(
                "".join(f"       {line}\n" for line in [*result["tail"], *(["       direct:", *result["direct_tail"]] if result["direct_tail"] else [])])
            )
    failures = sum(not result["correct"] for result in results)
    out = args.output_directory / f"{started:%Y%m%dT%H%M%SZ}_{os.getpid()}"
    out.mkdir(parents=True)
    revision = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip()
    dirty = bool(run_command("git", ["status", "--porcelain", "--", "scripts", "dbt", ".pre-commit-config.yaml"], cwd=ROOT, check=True).stdout.strip())
    report = {
        "started": started.isoformat(),
        "stable_code": stable,
        "scanner_exit": scanner_exit,
        "revision": revision,
        "uncommitted_changes": dirty,
        "checker": str(args.checker),
        "checker_sha256": sha256_of(args.checker),
        "hook_config_sha256": sha256_of(args.hook_config),
        "tools": {"python": sys.version.split()[0], "pre_commit": run_command(str(PRE_COMMIT), ["--version"], check=True).stdout.strip()},
        "workers": workers,
        "wall_seconds": round((datetime.now(UTC) - started).total_seconds(), 1),
        "passed": len(results) - failures,
        "total": len(results),
        "cases": results,
    }
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"{report['passed']} of {report['total']} cases behaved as expected; report {out / 'report.json'}\n")
    return int(bool(failures) or not stable or scanner_exit != 0)


if __name__ == "__main__":
    sys.exit(main())
