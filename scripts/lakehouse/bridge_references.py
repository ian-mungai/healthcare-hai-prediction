"""Write and check the reference inputs of the gold control bridge, and check the formula recipes against them (failure modes 800 to 803).

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.bridge_references --write    # write the four reference seeds in dbt/seeds/gold/
    .venv/bin/python -m scripts.lakehouse.bridge_references --check    # fail when a seed drifts or a recipe breaks a rule

``registry_controls.csv`` also carries each control's ``family``: the analytic domain (Structure, Process, Outcome, Context or Linkage) of the dimension of its
sources' original source families. A control with no source family takes the domain of ``registry_family_supplement.csv``, a reviewed list. A control whose
sources fall in two domains, a dimension that maps to no domain and a control that resolves to none or to two values stop the run.

``gold_contract_columns.csv`` is generated from the frozen gold contract (every column of every gold model with its explicit footnote column and
whether it is the model's published measure-key column). ``registry_controls.csv`` is generated from the registry of revision 3 and its append-only
additions. ``gold_measure_key_evidence.csv`` lists the published measure key of each ACS, impact, HAI hospital and BLS bridge row with the reviewed file that
supports it (the reviewed ACS variable map, the reviewed impact layout, the HAI measure types and parts or the reviewed BLS measure list).
``gold_measure_key_rules.csv`` declares how each long fact is validated. Every PLACES, DRG, HCAHPS, SVI and GV published key must be an exact,
case-sensitive member of its domain in ``dbt/seeds/gold_measure_id_domains.csv`` (the lead's bronze-derived seed, copied byte-identical; amendment A25),
with no fallback to registry text; an SVI or GV key must also not be listed by ``svi_identifier_columns()`` or ``gv_identifier_columns()``. The
care-compare facts (TIMELY, MATERNAL, UNPLANNED, COMPLICATIONS, HRRP) and the MMD condition labels (MMD) use domains of the extended seed (amendment
A27); only the HAI state and national facts, which no bridge row names, keep the structural checks of the bridge test. A bridge row with an empty key
(a held or closed control such as the C258 definition hold) is not a published key: it is checked by ``assert_bridge_rows_complete``. An unsupported key
stops the generator. The dbt tests of the bridge read these seeds because the dbt container sees only the project.

``--check`` also validates each recipe in dbt/seeds/features/: the file header and a non-empty body, the recipe-group inventory (``recipe_groups.csv``,
every group with its count of inputs per role), group-level required metadata (period, grouping and suppression rules; geography level, source status
and field mapping where the retired seeds required them), allowed rules and roles, contract columns, published-key predicates and the rule that every value
input is a bridge row of the same control. The contract is not committed (it sits in the ignored plans folder), so a run without it needs
``--contract-optional``; the checks that need it are then skipped, reported and never counted as a pass.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT = REPO_ROOT / "data_contracts/lakehouse/gold_contract.json"
REGISTRY = REPO_ROOT / "config/acquisition/source_registry.json"
ADDITIONS = REPO_ROOT / "config/acquisition/registry_additions.json"
GOLD_SEEDS = REPO_ROOT / "dbt/seeds/gold"
RECIPES = REPO_ROOT / "dbt/seeds/features"
SILVER_SEEDS = REPO_ROOT / "dbt/seeds"
HAI_MACRO = REPO_ROOT / "dbt/macros/hai_outcome_columns.sql"
DOMAIN_SEED = SILVER_SEEDS / "gold_measure_id_domains.csv"
DOMAIN_COLUMNS = ("domain", "published_key", "published_headers", "source_table", "source_column", "first_release", "last_release")
SVI_IDENTIFIER_MACRO = (REPO_ROOT / "dbt/macros/county_context_columns.sql", "svi_identifier_columns")
GV_IDENTIFIER_MACRO = (REPO_ROOT / "dbt/macros/county_health_columns.sql", "gv_identifier_columns")
REGISTRY_REVISION = 3
CONTRACT_COLUMNS = ("gold_model", "column_name", "footnote_column", "is_measure_key")
CONTROL_COLUMNS = ("measure_control", "parent_control", "origin", "family")
FAMILY_SUPPLEMENT = "registry_family_supplement.csv"
SUPPLEMENT_COLUMNS = ("measure_control", "family")
# The analytic domains a control's family takes; the dimension of a source family maps to one of them by its text before ":" or "/", or by the linkage list.
DOMAINS = ("Structure", "Process", "Outcome", "Context", "Linkage")
LINKAGE_DIMENSIONS = ("Geographic linkage", "Spatial context", "Community context")
EVIDENCE_COLUMNS = ("fact", "measure_key", "measure_control", "basis")
RULE_COLUMNS = ("fact", "rule", "value")
INVENTORY_COLUMNS = ("recipe", "measure_control", "source_row", "rule", "inputs")
# The column of a long fact that holds the published measure ID, field or concept (a reviewed list; each column must exist in the contract).
# A fact that is not listed is wide: its bridge rows name the column itself as the measure key.
LONG_FACT_KEYS = {
    "fct_hai_hospital_window": "measure_id",
    "fct_hai_state_window": "measure_id",
    "fct_hai_national_window": "measure_id",
    "fct_timely_effective_care_window": "measure_id",
    "fct_maternal_health_window": "measure_id",
    "fct_hcahps_window": "measure_id",
    "fct_unplanned_visits_window": "measure_id",
    "fct_complications_deaths_window": "measure_id",
    "fct_hrrp_window": "measure_id",
    "fct_acs_county_value": "concept_id",
    "fct_svi_county_value": "field",
    "fct_gv_county_value": "field",
    "fct_places_county_value": "measureid",
    "fct_bls_county_series": "measure",
    "fct_ipps_impact_value": "field",
    "fct_medicare_inpatient_drg": "drg_cd",
    "fct_mmd_prevalence": "condition_label",
}
# Long facts whose published key must be an exact member of a domain of the measure-ID domain seed, with the domain name (amendment A25).
FACT_DOMAINS = {
    "fct_places_county_value": "PLACES",
    "fct_medicare_inpatient_drg": "DRG",
    "fct_hcahps_window": "HCAHPS",
    "fct_svi_county_value": "SVI",
    "fct_gv_county_value": "GV",
    "fct_timely_effective_care_window": "TIMELY",
    "fct_maternal_health_window": "MATERNAL",
    "fct_unplanned_visits_window": "UNPLANNED",
    "fct_complications_deaths_window": "COMPLICATIONS",
    "fct_hrrp_window": "HRRP",
    "fct_mmd_prevalence": "MMD",
}
# Wide-source domains: the seed lists every published column, so a key must also not be a column that identifies a row.
IDENTIFIER_MACROS = {"SVI": SVI_IDENTIFIER_MACRO, "GV": GV_IDENTIFIER_MACRO}
# Long facts checked against a reviewed file of the repository (see key_domains).
EVIDENCE_FACTS = ("fct_acs_county_value", "fct_ipps_impact_value", "fct_hai_hospital_window", "fct_bls_county_series")
# Long facts with no authoritative domain: the bridge test applies only its structural checks to their keys (no bridge row names a key of these today).
STRUCTURAL_ONLY_FACTS = ("fct_hai_state_window", "fct_hai_national_window")
# Reviewed BLS measures; the income and labor bridge rows may name only these (the generator stops on any other key).
BLS_MEASURES = ("unemployment_rate", "unemployed", "employed", "labor_force")
KEY_SEPARATOR = "|"
# The words of the message that reports skipped checks; a run with such a message is never a pass.
SKIPPED_MARK = "were skipped"
COMMON_RECIPE_COLUMNS = (
    "measure_control",
    "rule",
    "role",
    "fact",
    "value_column",
    "key_column",
    "key_value",
    "join_keys",
    "aggregation",
    "guard",
    "group_by",
    "period",
    "suppression",
    "source_row",
)
RECIPE_EXTRAS = {
    "cost_report_measures": (),
    "impact_measures": (),
    "mup_measures": (),
    "occmix_measures": ("output_name", "definition", "source_status"),
    "hhs_onc_measures": ("field_mapping",),
    "mmd_conditions": ("geography_level", "condition_code", "shortage_source_row"),
    "shortage_measures": (),
}
RECIPE_RULES = {
    "cost_report_measures": {"value", "code", "ratio", "per_day", "ratio_per_day_denominator"},
    "impact_measures": {"value", "code", "ratio"},
    "mup_measures": {"value", "ratio", "drg_share", "hold"},
    "occmix_measures": {"value", "ratio", "none"},
    "hhs_onc_measures": {"closed", "ratio_of_sums", "sum_of_weekly_sums", "mean_of_weekly_averages", "flag_by_period", "distinct_text", "distinct_count"},
    "mmd_conditions": {"prevalence"},
    "shortage_measures": {"count_in_force", "max_score_in_force"},
}
# Roles each rule must have; "context" is always allowed and names columns the rule reads for grain, filters or joins.
REQUIRED_ROLES = {
    "value": {"value"},
    "code": {"value"},
    "ratio": {"numerator", "denominator"},
    "per_day": {"numerator", "divisor"},
    "ratio_per_day_denominator": {"numerator", "denominator", "divisor"},
    "drg_share": {"numerator", "denominator"},
    "ratio_of_sums": {"numerator", "denominator"},
    "sum_of_weekly_sums": {"value"},
    "mean_of_weekly_averages": {"value"},
    "flag_by_period": {"value"},
    "distinct_text": {"value"},
    "distinct_count": {"value"},
    "count_in_force": {"id"},
    "max_score_in_force": {"score"},
    "prevalence": {"value"},
    "hold": set(),
    "none": set(),
    "closed": set(),
}
# The one control whose prevalence group has no value input: the parent of the MMD condition controls, which carries only the columns they share.
MMD_PARENT_CONTROL = "C258"
# Metadata a group must carry on at least one of its rows, by rule (the grouping, period and suppression rules of the archived models).
REQUIRED_GROUP_METADATA = {
    "ratio_of_sums": ("group_by", "period"),
    "sum_of_weekly_sums": ("group_by", "period"),
    "mean_of_weekly_averages": ("group_by", "period"),
    "flag_by_period": ("group_by", "period", "suppression"),
    "distinct_text": ("group_by", "period"),
    "distinct_count": ("group_by", "period"),
    "count_in_force": ("group_by", "period", "suppression"),
    "max_score_in_force": ("group_by", "period", "suppression"),
}
# Columns the retired seed required (not null and, where listed, accepted values) on every row; checked on each group.
REQUIRED_VALUES = {
    "occmix_measures": {"source_status": {"available_conditional", "unavailable_source_definition"}},
    "hhs_onc_measures": {"field_mapping": {"exact", "api_field_name", "closed"}},
    "mmd_conditions": {"geography_level": {"county", "state"}, "condition_code": set()},
}
# Roles whose column the seed named for the control: each must be a bridge row of that control. The others (divisor, context) are rule inputs only.
BRIDGE_ROLES = {"value", "numerator", "denominator", "id", "score"}
AGGREGATIONS = {"", "sum", "avg", "max", "count_distinct", "bool_or", "string_agg_distinct"}


class ReferenceInputError(RuntimeError):
    """The contract or a registry file does not give clean reference inputs."""


def load_contract(path: Path) -> dict[str, Any]:
    """Return the frozen gold contract."""
    data: dict[str, Any] = json.loads(path.read_text())
    return data


def model_columns(contract: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Return each gold model's columns in contract order: facts first, then the dimensions and bridges (their columns or attribute names)."""
    models: dict[str, list[dict[str, Any]]] = {}
    for name in sorted(contract["facts"]):
        models[name] = list(contract["facts"][name]["columns"])
    for name in sorted(contract["dimensions_and_bridges"]):
        entry = contract["dimensions_and_bridges"][name]
        if "columns" in entry:
            models[name] = list(entry["columns"])
        else:
            models[name] = [{"name": attribute} for attribute in entry["attributes"]]
    return models


def contract_rows(contract: Mapping[str, Any]) -> list[dict[str, str]]:
    """Return one row per model column with its footnote column and measure-key flag; a column listed twice or a missing footnote column stops the run."""
    models = model_columns(contract)
    for fact, key in LONG_FACT_KEYS.items():
        if fact not in models or key not in {column["name"] for column in models[fact]}:
            raise ReferenceInputError(f"measure-key column {key} is not a column of {fact} in the contract")
    rows: list[dict[str, str]] = []
    for model, columns in models.items():
        names = [column["name"] for column in columns]
        if len(set(names)) != len(names):
            raise ReferenceInputError(f"{model} lists a column twice")
        for column in columns:
            footnote = column.get("footnote") or ""
            if footnote and footnote not in names:
                raise ReferenceInputError(f"{model}.{column['name']} names footnote column {footnote}, which {model} does not have")
            flag = "true" if LONG_FACT_KEYS.get(model) == column["name"] else "false"
            rows.append({"gold_model": model, "column_name": column["name"], "footnote_column": footnote, "is_measure_key": flag})
    return rows


def registry_records(registry: Mapping[str, Any], additions: Mapping[str, Any]) -> dict[str, tuple[str, dict[str, Any]]]:
    """Return each registry control with its origin; a wrong revision, a duplicate ID or an orphan addition stops the run."""
    if registry["registry_revision"] != REGISTRY_REVISION:
        raise ReferenceInputError(f"the registry is revision {registry['registry_revision']}, not {REGISTRY_REVISION}")
    records: dict[str, tuple[str, dict[str, Any]]] = {}
    for origin, controls in (("source_registry", registry["measure_controls"]), ("registry_additions", additions["measure_controls"])):
        for control in controls:
            if control["id"] in records:
                raise ReferenceInputError(f"control {control['id']} appears twice (the second time in {origin})")
            records[control["id"]] = (origin, control)
    for control_id, (_, control) in records.items():
        parent = control.get("parent_id") or ""
        if parent and parent not in records:
            raise ReferenceInputError(f"control {control_id} names parent {parent}, which is not in the registry")
    return records


def domain_of(dimension: str) -> str | None:
    """Return the analytic domain of a source family's dimension text, or None when it maps to no domain."""
    if dimension in LINKAGE_DIMENSIONS:
        return "Linkage"
    head = re.split(r"[:/]", dimension)[0].strip()
    return head if head in DOMAINS and head != "Linkage" else None


def supplement_problems(supplement: list[dict[str, str]], records: Mapping[str, tuple[str, dict[str, Any]]]) -> tuple[dict[str, str], list[str]]:
    """Return the reviewed supplement by control and what is wrong with it: a control twice, not in the registry or a family outside the domains."""
    problems: list[str] = []
    by_control: dict[str, str] = {}
    for row in supplement:
        control, family = row["measure_control"], row["family"]
        if control in by_control:
            problems.append(f"{FAMILY_SUPPLEMENT}: control {control} appears twice")
        if control not in records:
            problems.append(f"{FAMILY_SUPPLEMENT}: control {control} is not in the registry")
        if family not in DOMAINS:
            problems.append(f"{FAMILY_SUPPLEMENT}: control {control} has family {family!r}, not one of {list(DOMAINS)}")
        by_control[control] = family
    return by_control, problems


def control_families(
    registry: Mapping[str, Any], records: Mapping[str, tuple[str, dict[str, Any]]], supplement: list[dict[str, str]]
) -> tuple[dict[str, str], list[str]]:
    """Return each control's family and why a control does not resolve to exactly one: an unmapped dimension, an unknown source, two domains or none."""
    reviewed, problems = supplement_problems(supplement, records)
    domains: dict[str, str | None] = {}
    for family in registry["source_families"]:
        domains[family["id"]] = domain_of(family["dimension"])
        if domains[family["id"]] is None:
            problems.append(f"source family {family['id']} has dimension {family['dimension']!r}, which maps to no domain")
    sources = {source["source_id"]: source for source in registry["sources"]}
    families: dict[str, str] = {}
    for control_id, (_, control) in records.items():
        found: set[str] = set()
        for source_id in control.get("source_ids") or []:
            if source_id not in sources:
                problems.append(f"control {control_id} names source {source_id}, which is not in the registry")
                continue
            for family_id in sources[source_id].get("original_source_family_ids") or []:
                if family_id not in domains:
                    problems.append(f"source {source_id} names source family {family_id}, which is not in the registry")
                elif domains[family_id]:
                    found.add(str(domains[family_id]))
        if len(found) > 1:
            problems.append(f"control {control_id} resolves to the domains {sorted(found)}, not one")
        elif found and control_id in reviewed:
            problems.append(f"{FAMILY_SUPPLEMENT}: control {control_id} has a source family; remove it from the supplement")
        elif found:
            families[control_id] = found.pop()
        elif control_id in reviewed:
            families[control_id] = reviewed[control_id]
        else:
            problems.append(f"control {control_id} has no source family and no row in {FAMILY_SUPPLEMENT}")
    return families, problems


def registry_rows(records: Mapping[str, tuple[str, dict[str, Any]]], families: Mapping[str, str]) -> list[dict[str, str]]:
    """Return one row per registry control with its parent, origin and family; a control that resolves to no family holds an empty family."""
    rows = [
        {"measure_control": control_id, "parent_control": control.get("parent_id") or "", "origin": origin, "family": families.get(control_id, "")}
        for control_id, (origin, control) in records.items()
    ]
    return sorted(rows, key=lambda row: (row["origin"], row["measure_control"]))


def as_csv(columns: Iterable[str], rows: Iterable[Mapping[str, str]]) -> str:
    """Return the rows as CSV text with Unix line endings."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(columns), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def read_table(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Return a CSV file's header and rows; an empty file has an empty header."""
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def read_rows(path: Path) -> list[dict[str, str]]:
    """Return a CSV file's rows."""
    return read_table(path)[1]


def tokens(text: str, separator: str | None = None) -> list[str]:
    """Return the non-empty parts of a space-separated (or separator-split) list."""
    return [part for part in (text.split(separator) if separator else text.split()) if part]


def normalized(text: str) -> str:
    """Return the text lower-cased with every run of other characters as one space, padded so a whole-word match can use ``in``."""
    return f" {re.sub(r'[^a-z0-9]+', ' ', text.lower()).strip()} "


def hai_keys() -> set[str]:
    """Return the published HAI measure IDs: each infection type with each part, read from the macros that define them."""
    text = HAI_MACRO.read_text()
    types = re.findall(r"'(HAI_\d+)'", text)
    parts = re.findall(r"\['([A-Z]+)',\s*'[a-z_]+'\]", text)
    return {f"{kind}_{part}" for kind in types for part in parts}


def key_domains() -> dict[str, tuple[str, set[str]]]:
    """Return the reviewed measure-key domains by fact: the basis name and the allowed keys."""
    acs = {row["concept_id"] for row in read_rows(SILVER_SEEDS / "acs_variable_map.csv")}
    impact = {row["field"] for row in read_rows(SILVER_SEEDS / "impact_layout_columns.csv")}
    return {
        "fct_acs_county_value": ("acs_variable_map", acs),
        "fct_ipps_impact_value": ("impact_layout_columns", impact),
        "fct_hai_hospital_window": ("hai_measure_parts", hai_keys()),
        "fct_bls_county_series": ("bls_measure_list", set(BLS_MEASURES)),
    }


def measure_domains() -> dict[str, set[str]]:
    """Return the exact published keys of each domain of the measure-ID domain seed; a missing, reshaped or incomplete seed stops the run."""
    if not DOMAIN_SEED.exists():
        raise ReferenceInputError(f"{DOMAIN_SEED} is missing")
    header, rows = read_table(DOMAIN_SEED)
    if header != list(DOMAIN_COLUMNS):
        raise ReferenceInputError(f"{DOMAIN_SEED.name} has columns {header}, expected {list(DOMAIN_COLUMNS)}")
    domains: dict[str, set[str]] = {}
    for row in rows:
        domains.setdefault(row["domain"], set()).add(row["published_key"])
    missing = sorted(set(FACT_DOMAINS.values()) - set(domains))
    if missing or not all(domains[name] for name in FACT_DOMAINS.values()):
        raise ReferenceInputError(f"{DOMAIN_SEED.name} lacks keys for the domains {missing or sorted(FACT_DOMAINS.values())}")
    return domains


def identifier_columns(macro: tuple[Path, str]) -> list[str]:
    """Return the columns a silver identifier macro lists (the quoted names of its ``return([...])``); a missing or empty list stops the run."""
    path, name = macro
    text = path.read_text()
    start = text.find(f"macro {name}(")
    end = text.find("endmacro", start)
    body = text[start:end] if start >= 0 and end > start else ""
    returned = body.partition("return(")[2]
    columns = re.findall(r"'([^']+)'", returned)
    if not columns:
        raise ReferenceInputError(f"{path.name} no longer lists columns in {name}()")
    return columns


def key_rules() -> list[dict[str, str]]:
    """Return the validation rule rows of every long fact: a domain with its identifier columns, a reviewed-file check or structural only."""
    groups = (*FACT_DOMAINS, *EVIDENCE_FACTS, *STRUCTURAL_ONLY_FACTS)
    if sorted(groups) != sorted(LONG_FACT_KEYS):
        raise ReferenceInputError(f"the key rules cover {sorted(groups)}, not the long facts {sorted(LONG_FACT_KEYS)}")
    rows = [{"fact": fact, "rule": "domain", "value": domain} for fact, domain in FACT_DOMAINS.items()]
    for fact, domain in FACT_DOMAINS.items():
        if domain in IDENTIFIER_MACROS:
            rows += [{"fact": fact, "rule": "identifier", "value": column} for column in identifier_columns(IDENTIFIER_MACROS[domain])]
    rows += [{"fact": fact, "rule": "evidence", "value": ""} for fact in EVIDENCE_FACTS]
    rows += [{"fact": fact, "rule": "structural_only", "value": ""} for fact in STRUCTURAL_ONLY_FACTS]
    return sorted(rows, key=lambda row: (row["fact"], row["rule"], row["value"]))


def domain_problem(control: str, fact: str, key: str, domains: Mapping[str, set[str]]) -> str:
    """Return why a published key is not accepted for a domain fact, or an empty string when it is accepted."""
    domain = FACT_DOMAINS[fact]
    prefix = f"{control}: no authoritative source supports measure key {key!r} of {fact}"
    if key not in domains[domain]:
        near = sorted(known for known in domains[domain] if known.lower() == key.lower())
        hint = f"; it differs only in case from {near[0]!r}" if near else ""
        return f"{prefix}: not an exact member of the {domain} domain{hint}"
    if domain in IDENTIFIER_MACROS and key in identifier_columns(IDENTIFIER_MACROS[domain]):
        return f"{prefix}: {key!r} identifies a row of the {domain} source (listed by {IDENTIFIER_MACROS[domain][1]}())"
    return ""


def key_evidence(bridge: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[str]]:
    """Return the evidence row of each reviewed-file key and the keys of any long fact that no authoritative source supports."""
    reviewed = key_domains()
    domains = measure_domains()
    evidence: dict[tuple[str, str, str], dict[str, str]] = {}
    problems: list[str] = []
    for row in bridge:
        fact, key, control = row["fact"], row["measure_key"], row["measure_control"]
        if fact not in LONG_FACT_KEYS or not key:
            continue
        if fact in FACT_DOMAINS:
            problem = domain_problem(control, fact, key, domains)
            if problem:
                problems.append(problem)
        elif fact in reviewed:
            if key in reviewed[fact][1]:
                evidence[(fact, key, control)] = {"fact": fact, "measure_key": key, "measure_control": control, "basis": reviewed[fact][0]}
            else:
                problems.append(f"{control}: no authoritative source supports measure key {key!r} of {fact}: not in the {reviewed[fact][0]} list")
    return [evidence[name] for name in sorted(evidence)], problems


def parse_inputs(text: str) -> Counter[str]:
    """Return the role counts of an inventory row (``numerator=1 denominator=1``)."""
    return Counter({role: int(count) for role, count in (part.split("=") for part in text.split())})


def group_roles(rows: list[dict[str, str]]) -> dict[tuple[str, str, str], Counter[str]]:
    """Return each recipe group (control, source row and rule) with the count of its inputs per role."""
    groups: dict[tuple[str, str, str], Counter[str]] = {}
    for row in rows:
        counter = groups.setdefault((row["measure_control"], row["source_row"], row["rule"]), Counter())
        if row["role"]:
            counter[row["role"]] += 1
    return groups


def recipe_problems(
    name: str,
    table: tuple[list[str], list[dict[str, str]]],
    inventory: list[dict[str, str]],
    bridge: list[dict[str, str]],
    controls: set[str],
    models: dict[str, set[str]] | None,
) -> list[str]:
    """Return what breaks the rules in one recipe: header, inventory, cardinalities, metadata, vocabulary, columns, keys and the bridge link."""
    header, rows = table
    path = f"{name}_recipe.csv"
    problems: list[str] = []
    expected = list(COMMON_RECIPE_COLUMNS + RECIPE_EXTRAS[name])
    if header != expected:
        problems.append(f"{path}: columns are {header}, expected {expected}")
    if not rows:
        problems.append(f"{path}: has no rows")
    for row in rows:
        problems += _row_problems(path, row, controls, models)
    groups = group_roles(rows)
    wanted = {(item["measure_control"], item["source_row"], item["rule"]): parse_inputs(item["inputs"]) for item in inventory if item["recipe"] == name}
    for group in sorted(set(wanted) - set(groups)):
        problems.append(f"{path}: group {group} of the inventory is missing")
    for group in sorted(set(groups) - set(wanted)):
        problems.append(f"{path}: group {group} is not in the inventory")
    for group in sorted(set(groups) & set(wanted)):
        if groups[group] != wanted[group]:
            problems.append(f"{path}: group {group} has inputs {dict(groups[group])}, the inventory says {dict(wanted[group])}")
    by_group: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        by_group.setdefault((row["measure_control"], row["source_row"], row["rule"]), []).append(row)
    for group, members in by_group.items():
        problems += _group_problems(name, path, group, members)
    problems += _bridge_problems(path, rows, bridge)
    return problems


def _group_problems(name: str, path: str, group: tuple[str, str, str], members: list[dict[str, str]]) -> list[str]:
    """Return the problems of one recipe group: required roles and the metadata the retired seed or the archived model required."""
    control, source_row, rule = group
    where = f"{path}: {control} row {source_row} rule {rule}"
    problems: list[str] = []
    roles = {row["role"] for row in members} - {""}
    need = set(REQUIRED_ROLES.get(rule, set()))
    parent = rule == "prevalence" and control == MMD_PARENT_CONTROL
    if parent:
        need = set()
    if not need <= roles:
        problems.append(f"{where}: lacks roles {sorted(need - roles)}")
    if roles - need - {"context"}:
        problems.append(f"{where}: has roles {sorted(roles - need - {'context'})} it does not use")
    if not need and not parent and roles:
        problems.append(f"{where}: takes no inputs")
    for column in REQUIRED_GROUP_METADATA.get(rule, ()):
        if not any(row[column] for row in members):
            problems.append(f"{where}: no row carries {column}")
    for column, allowed in REQUIRED_VALUES.get(name, {}).items():
        if parent:
            continue
        values = {row[column] for row in members if row[column]}
        if not values or (allowed and not values <= allowed):
            problems.append(f"{where}: {column} is {sorted(values) or 'blank'}, it must be one of {sorted(allowed) or 'a value'}")
    if name == "occmix_measures" and rule != "none" and not any(row["output_name"] for row in members):
        problems.append(f"{where}: no row carries output_name")
    return problems


def _row_problems(path: str, row: dict[str, str], controls: set[str], models: dict[str, set[str]] | None) -> list[str]:
    """Return the problems of one recipe row that need no other row."""
    where = f"{path}: {row['measure_control']} row {row['source_row']}"
    problems: list[str] = []
    if row["measure_control"] not in controls:
        problems.append(f"{where}: control is not in the registry")
    if row["rule"] not in RECIPE_RULES[path.removesuffix("_recipe.csv")]:
        problems.append(f"{where}: rule {row['rule']!r} is not allowed")
    if row["aggregation"] not in AGGREGATIONS:
        problems.append(f"{where}: aggregation {row['aggregation']!r} is not allowed")
    if row["role"] and row["role"] not in BRIDGE_ROLES | {"divisor", "context"}:
        problems.append(f"{where}: role {row['role']!r} is not allowed")
    if bool(row["role"]) != bool(row["value_column"]) or bool(row["role"]) != bool(row["fact"]):
        problems.append(f"{where}: role, fact and value column come together or not at all")
    if row["role"] and row["role"] != "context" and len(row["value_column"].split()) != 1:
        problems.append(f"{where}: role {row['role']} names exactly one column")
    if bool(row["key_column"]) != bool(row["key_value"]):
        problems.append(f"{where}: key column and key value come together or not at all")
    if row["key_column"] and row["key_column"] != LONG_FACT_KEYS.get(row["fact"]):
        problems.append(f"{where}: {row['key_column']} is not the measure-key column of {row['fact']}")
    if row["role"] in BRIDGE_ROLES and (row["fact"] in LONG_FACT_KEYS) != bool(row["key_column"]):
        problems.append(f"{where}: a long fact needs a published-key predicate and a wide fact has none")
    if models is not None and row["fact"]:
        if row["fact"] not in models:
            problems.append(f"{where}: {row['fact']} is not in the gold contract")
        else:
            for column in [*row["value_column"].split(), *row["join_keys"].split(), *row["group_by"].split()]:
                if column not in models[row["fact"]]:
                    problems.append(f"{where}: {column} is not a column of {row['fact']}")
    return problems


def _bridge_problems(path: str, rows: list[dict[str, str]], bridge: list[dict[str, str]]) -> list[str]:
    """Return each value input that the control's bridge rows do not carry, and each recipe control with no bridge row."""
    problems: list[str] = []
    by_control: dict[str, set[tuple[str, str, str]]] = {}
    for row in bridge:
        by_control.setdefault(row["measure_control"], set()).add((row["fact"], row["measure_key"], row["value_column"]))
    for row in rows:
        carried = by_control.get(row["measure_control"])
        if carried is None:
            problems.append(f"{path}: {row['measure_control']} has no bridge row")
            continue
        if row["role"] not in BRIDGE_ROLES:
            continue
        keys = tokens(row["key_value"], KEY_SEPARATOR) or [row["value_column"]]
        for key in keys:
            if (row["fact"], key, row["value_column"]) not in carried:
                problems.append(
                    f"{path}: {row['measure_control']} row {row['source_row']} reads {row['fact']}.{row['value_column']} ({key}), not in its bridge rows"
                )
    return problems


def generated(contract_path: Path, contract_optional: bool, seeds_dir: Path) -> tuple[dict[str, str], dict[str, set[str]] | None, list[str]]:
    """Return the text each reference seed must have, the contract's columns per model (None when the contract is absent and optional) and key problems."""
    registry = json.loads(REGISTRY.read_text())
    records = registry_records(registry, json.loads(ADDITIONS.read_text()))
    supplement_header, supplement = read_table(seeds_dir / FAMILY_SUPPLEMENT) if (seeds_dir / FAMILY_SUPPLEMENT).exists() else ([], [])
    if supplement_header != list(SUPPLEMENT_COLUMNS):
        raise ReferenceInputError(f"{FAMILY_SUPPLEMENT} has columns {supplement_header}, expected {list(SUPPLEMENT_COLUMNS)}")
    families, family_problems = control_families(registry, records, supplement)
    texts = {"registry_controls.csv": as_csv(CONTROL_COLUMNS, registry_rows(records, families))}
    bridge = [row for path in sorted(seeds_dir.glob("*_bridge.csv")) for row in read_rows(path)]
    evidence, key_problems = key_evidence(bridge)
    key_problems = [*family_problems, *key_problems]
    texts["gold_measure_key_evidence.csv"] = as_csv(EVIDENCE_COLUMNS, evidence)
    texts["gold_measure_key_rules.csv"] = as_csv(RULE_COLUMNS, key_rules())
    if not contract_path.exists():
        if not contract_optional:
            raise ReferenceInputError(f"{contract_path} is missing; pass --contract-optional to check only what does not need the contract")
        return texts, None, key_problems
    contract = load_contract(contract_path)
    # The contract passed with --contract must name the same measure-key column per long fact as the reviewed list (gate notes 14 and 32).
    stated = {name: fact["measure_key_column"] for name, fact in contract["facts"].items() if fact.get("measure_key_column")}
    if stated != LONG_FACT_KEYS:
        key_problems = [*key_problems, f"{contract_path.name}: measure_key_column per long fact differs from LONG_FACT_KEYS"]
    rows = contract_rows(contract)
    texts["gold_contract_columns.csv"] = as_csv(CONTRACT_COLUMNS, rows)
    models: dict[str, set[str]] = {}
    for row in rows:
        models.setdefault(row["gold_model"], set()).add(row["column_name"])
    return texts, models, key_problems


def check(contract_path: Path, contract_optional: bool, seeds_dir: Path = GOLD_SEEDS, recipes_dir: Path = RECIPES) -> list[str]:
    """Return every drift of a reference seed and every broken recipe rule."""
    texts, models, problems = generated(contract_path, contract_optional, seeds_dir)
    problems = list(problems)
    for name, text in texts.items():
        if not (seeds_dir / name).exists() or (seeds_dir / name).read_text() != text:
            problems.append(f"{name} differs from what the authoritative files generate")
    if models is None:
        problems.append(f"gold_contract_columns.csv and the contract-based recipe checks {SKIPPED_MARK}: the contract is absent")
    controls = {row["measure_control"] for row in read_rows(seeds_dir / "registry_controls.csv")} if (seeds_dir / "registry_controls.csv").exists() else set()
    bridge = [row for path in sorted(seeds_dir.glob("*_bridge.csv")) for row in read_rows(path)]
    inventory_path = recipes_dir / "recipe_groups.csv"
    if not inventory_path.exists():
        return [*problems, "recipe_groups.csv is missing"]
    header, inventory = read_table(inventory_path)
    if header != list(INVENTORY_COLUMNS) or not inventory:
        problems.append(f"recipe_groups.csv: columns are {header} with {len(inventory)} rows, expected {list(INVENTORY_COLUMNS)} and rows")
    for name in RECIPE_RULES:
        path = recipes_dir / f"{name}_recipe.csv"
        if not path.exists():
            problems.append(f"{path.name} is missing")
            continue
        problems += recipe_problems(name, read_table(path), inventory, bridge, controls, models)
    return problems


def main() -> int:
    """Write or check the reference seeds; a check fails on drift and on a broken recipe."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="write the four reference seeds")
    mode.add_argument("--check", action="store_true", help="fail when a seed drifts from the authoritative files or a recipe breaks a rule")
    parser.add_argument("--contract", type=Path, default=CONTRACT, help="the frozen gold contract")
    parser.add_argument("--contract-optional", action="store_true", help="skip the checks that need the contract when it is absent")
    args = parser.parse_args()
    try:
        if args.write:
            texts, _, key_problems = generated(args.contract, False, GOLD_SEEDS)
            if key_problems:
                raise ReferenceInputError("; ".join(key_problems))
            for name, text in texts.items():
                (GOLD_SEEDS / name).write_text(text)
            sys.stdout.write(f"bridge references: wrote {', '.join(sorted(texts))}\n")
            return 0
        problems = check(args.contract, args.contract_optional)
    except ReferenceInputError as error:
        sys.stderr.write(f"bridge references: {error}\n")
        return 1
    skipped = [problem for problem in problems if SKIPPED_MARK in problem]
    hard = [problem for problem in problems if SKIPPED_MARK not in problem]
    for problem in problems:
        sys.stderr.write(f"bridge references: {problem}\n")
    if not hard and skipped:
        sys.stdout.write("bridge references: no failure in the checks that ran, but checks were skipped (see above); this is not a pass\n")
    elif not hard:
        sys.stdout.write("bridge references: seeds reproduced and recipes valid\n")
    return 1 if hard else 0


if __name__ == "__main__":
    raise SystemExit(main())
