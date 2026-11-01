"""Semantic checks over DuckDB's parse of compiled gold SQL.

``json_serialize_sql`` gives each compiled model as a syntax tree. This module resolves column lineage through CTEs, subqueries and
projections (each base column keeps the relation it comes from), lists the physical relations a model reads (local CTEs excluded) and
applies the expression policy:

* The contract's ``expression_allowlist`` entries switch on the surrogate-key expressions (``md5``, the unknown-member ``coalesce``, the
  ``varchar`` cast and the key ``case``); with an entry missing, every expression that needs it fails.
* A key expression is accepted only at its contracted output role: a dimension's ``<name>_sk`` hashes that dimension's natural key
  (``DIMENSION_KEYS``), a bridge's key columns hash theirs (``BRIDGE_KEYS``), a fact's key column is ``coalesce(<dimension>.<dimension>_sk,
  md5('<unknown>'))``, and a fact's key join hashes one of the fact's own natural keys. An alias suffix, a CTE name or ``md5`` alone grants
  nothing, and a role matches by shape and by the names of the columns it reads.
* Scoped permissions apply to named models and output roles; ``DATE_ROLES`` and the other constants
  below name each one. Everything else that computes a value fails: arithmetic, ``case``, aggregates, window expressions, casts and any function.
* The allowlist entries that name a model (``STATE_PREFIX``, ``RANGE_BOUNDS``, ``COUNTY_MEMBERSHIP``, ``TRACT_COUNT``, ``EDITION_YEAR`` and
  ``COUNTY_FILTER``) each admit one expression: a plain aggregate (no filter, distinct or order by) or the exact function, comparison or test, in
  the named model, over the named relation and column by resolved lineage, in the named clause or select item and into the named output column only.
  Each change from that form is refused, and so is the expression with its entry missing.
* Six of those capabilities rest on the whole query of their model, and a property of the query cannot be proved in general (a later filter, a
  restricted row set, a borrowed row, a swapped list name each keep the evidence while changing the meaning). ``PINS`` therefore grants
  ``label_bounds`` (``dim_rurality_code``, ``dim_measure``), ``county_list_bounds`` (``dim_county``), ``edition_year`` (``bridge_measure_control``),
  ``state_prefix`` (``dim_state``) and ``tract_count`` and ``county_prefix`` (``bridge_county_planning_region``) only when the model's statement
  equals the delivered one: ``query_fingerprint`` is the hash of its parse without source positions and without the catalog and schema of a table
  reference. Any other structure gets no grant and the finding names both fingerprints. The structural rules above stay as a second condition. A
  change to a pinned model changes its pin and its probes deliberately.
* A key or an identity text may add the edition part (``KeySpec.edition``): the first release date or vintage that carries a published label. A row with
  no edition has a null one, which ``concat_ws`` drops, so the part may be left out only where the same select outputs a typed null edition. The
  bound label (``min``) reaches a key only as that part. The published-label selection accepts one more complete form: one row per published label of a
  code, grouped by the keys and the label, bounded by ``min`` and ``max`` of one date that is filtered as not null.
* Every expression node in the parse is visited or recognized by a role; a node nothing visited is reported, so an unsupported shape cannot hide.
* A function or window call written with a schema or catalog (``spoof."left"(...)``, ``system.main.count_star()``) is a finding wherever it stands: every
  permission above is for the unqualified built-in name, and a function in another namespace never receives it.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, NamedTuple

from jinja2 import Environment, StrictUndefined, select_autoescape
from jinja2.runtime import Macro

Json = dict[str, Any]
Base = tuple[str, str]
Matcher = Callable[[Json, "Scope"], "Value | None"]

UNKNOWN = "<unknown>"
# The models whose unknown member may carry null natural key parts (gold contract unknown_member rule).
NULL_SENTINEL_MODELS = frozenset({"dim_measure", "dim_date", "dim_hospital_owner_history"})
ARITHMETIC = frozenset({"+", "-", "*", "/", "%", "//", "**", "add", "subtract", "multiply", "divide", "mod"})
BOOLEAN_TYPES = frozenset(
    {"OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "OPERATOR_NOT", "COMPARE_IN", "COMPARE_NOT_IN", "COMPARE_BETWEEN", "COMPARE_NOT_BETWEEN"}
)
QUERY_NODES = frozenset({"SELECT_NODE", "SET_OPERATION_NODE"})
# Contract allowlist text to the capability it switches on (gold_contract.json expression_allowlist).
CAPABILITIES = {
    "md5(<natural key text>) for a surrogate key": "md5_key",
    "coalesce(<dimension>.<dimension>_sk, md5('<unknown>')) for the unknown member": "unknown_coalesce",
    "cast(<natural key> as varchar) inside a surrogate key": "varchar_cast",
    "case only inside surrogate-key or unknown-member handling": "key_case",
    "min and max over a label group's release date, data year or vintage, inside dim_rurality_code and dim_measure only": "label_bounds",
    "left(county_fips, 2) for the state FIPS prefix, inside dim_state only": "state_prefix",
    "min and max over county-list vintages and a membership test against the latest vintage's county list, inside dim_county only": "county_list_bounds",
    "count(*) of tract rows per old county and planning region, inside bridge_county_planning_region only": "tract_count",
    "left(edition_start, 4) and left(edition_end, 4) compared with the seed vintage range, inside bridge_measure_control only": "edition_year",
    "county_fips like '09%' for the old Connecticut counties, inside bridge_county_planning_region only": "county_prefix",
}


class KeySpec(NamedTuple):
    """The columns whose text a key hashes, in order; several columns are joined with ``separator`` by ``concat_ws``.

    ``edition`` names a trailing part a key may add: the edition of a row whose published label changed. A row with no edition has a null one, and
    ``concat_ws`` drops a null, so a key without the part is the key the row had before editions existed. ``edition_from`` is the one stored column a
    key may hash as that part without computing it.
    """

    columns: tuple[str, ...]
    separator: str = "|"
    edition: str = ""
    edition_from: Base | None = None


EDITION_COLUMN = "edition_start"
EDITION_SOURCE = ("dim_measure", EDITION_COLUMN)
# Natural keys follow gold_contract.json and the unknown_member_natural_key tests. Code lists key on code system and code.
CODE_LIST = KeySpec(("code_system", "code"))
DIMENSION_KEYS = {
    "dim_census_tract": KeySpec(("census_tract_geoid",)),
    "dim_change_type": CODE_LIST,
    "dim_condition": CODE_LIST,
    "dim_county": KeySpec(("county_fips",)),
    "dim_date": KeySpec(("calendar_date",)),
    "dim_demographic_group": CODE_LIST,
    "dim_drg": CODE_LIST,
    "dim_ehr_product": CODE_LIST,
    "dim_enrollment": KeySpec(("enrollment_id",)),
    "dim_footnote": CODE_LIST,
    "dim_hospital": KeySpec(("ccn",)),
    "dim_hospital_owner_history": KeySpec(("ccn", "associate_id_owner", "role_code", "valid_from")),
    "dim_measure": KeySpec(("fact", "measure_key", "value_column"), edition=EDITION_COLUMN),
    "dim_owner_organization": KeySpec(("associate_id",)),
    "dim_owner_role": CODE_LIST,
    "dim_registry_control": KeySpec(("measure_control",)),
    "dim_release": KeySpec(("member_sha256",)),
    "dim_rurality_code": KeySpec(CODE_LIST.columns, edition=EDITION_COLUMN),
    "dim_source": KeySpec(("dataset_id",)),
    "dim_state": KeySpec(("state_code",)),
    "dim_zip": KeySpec(("zip_code",)),
}
# the control bridge keys its measure on the measure identity and its control on the control ID.
BRIDGE_KEYS = {
    "bridge_measure_control": {
        "measure_sk": KeySpec(("fact", "measure_key", "value_column"), edition=EDITION_COLUMN, edition_from=EDITION_SOURCE),
        "registry_control_sk": KeySpec(("measure_control",)),
    }
}
# dim_measure also carries its identity text, the natural key its key hashes.
IDENTITY_OUTPUTS = {"dim_measure": ("measure_identity", KeySpec(("fact", "measure_key", "value_column"), edition=EDITION_COLUMN))}


class RangeBound(NamedTuple):
    """An allowlist entry for ``min`` and ``max``: the capability, the one column of each relation it reads, the output column each result may reach and
    the aggregates that read the rows of the grouped columns themselves."""

    capability: str
    column: str
    relations: frozenset[str]
    minimum: str
    maximum: str
    grouped_rows: frozenset[str]

    @property
    def inputs(self) -> frozenset[Base]:
        return frozenset((relation, self.column) for relation in self.relations)


NAMED_MEASURE_MODELS = frozenset(
    {
        "int_cc_complications_deaths_windows",
        "int_cc_hcahps_windows",
        "int_cc_maternal_windows",
        "int_cc_timely_effective_windows",
        "int_cc_unplanned_visits_windows",
        "int_hai_hospital_windows",
        "int_hai_national_windows",
        "int_hai_state_windows",
    }
)
COUNTY_LIST_MODELS = frozenset({"int_acs_county_values", "int_county_adjacency_edges", "int_rucc_county_codes"})
RANGE_BOUNDS = {
    "dim_county": RangeBound("county_list_bounds", "vintage", COUNTY_LIST_MODELS, "first_unlisted_vintage", "last_listed_vintage", frozenset({"max"})),
    "dim_measure": RangeBound("label_bounds", "release_date", NAMED_MEASURE_MODELS, "edition_start", "edition_end", frozenset({"min", "max"})),
    "dim_rurality_code": RangeBound("label_bounds", "vintage", frozenset({"int_rucc_county_codes"}), "edition_start", "edition_end", frozenset({"min", "max"})),
}


class StatePrefix(NamedTuple):
    """The state FIPS prefix: its model, the relation and column it reads, the prefix width and the one output column."""

    model: str
    relation: str
    column: str
    width: int
    output: str


class CountyMembership(NamedTuple):
    """The membership test of dim_county: its model, the county code column, the output column and the list that names the latest members."""

    model: str
    column: str
    output: str
    list_name: str


class TractCount(NamedTuple):
    """The tract count: its model, the relation of the tract rows, the grouping (old county, planning region, file) and the output column."""

    model: str
    relation: str
    grouping: frozenset[str]
    output: str


class EditionYear(NamedTuple):
    """The edition join of the control bridge: its model, the edition relation, the year width, each bound's comparison and seed column, the seeds."""

    model: str
    relation: str
    width: int
    pairs: dict[str, tuple[str, str]]
    seeds: frozenset[str]


class CountyFilter(NamedTuple):
    """The filter of the old Connecticut counties: its model, the relation and column it reads and the one pattern."""

    model: str
    relation: str
    column: str
    pattern: str


STATE_PREFIX = StatePrefix("dim_state", "int_rucc_county_codes", "county_fips", 2, "state_fips")
COUNTY_MEMBERSHIP = CountyMembership("dim_county", "county_fips", "in_latest_county_list", "int_county_adjacency_edges")
TRACT_COUNT = TractCount("bridge_county_planning_region", "int_ruca_codes", frozenset({"county_fips", "county_fips_2023", "member_sha256"}), "tract_count")
EDITION_YEAR = EditionYear(
    "bridge_measure_control",
    "dim_measure",
    4,
    {"edition_end": ("COMPARE_GREATERTHANOREQUALTO", "vintage_start"), "edition_start": ("COMPARE_LESSTHANOREQUALTO", "vintage_end")},
    frozenset(
        {
            "acs_svi_measures_bridge",
            "cost_report_measures_bridge",
            "county_health_measures_bridge",
            "geography_measures_bridge",
            "hai_outcome_measures_bridge",
            "hhs_onc_measures_bridge",
            "impact_measures_bridge",
            "income_labor_measures_bridge",
            "mmd_conditions_bridge",
            "mup_measures_bridge",
            "occmix_measures_bridge",
            "ownership_measures_bridge",
            "registry_measure_sources_bridge",
            "shortage_measures_bridge",
            "validation_measures_bridge",
        }
    ),
)
COUNTY_FILTER = CountyFilter("bridge_county_planning_region", "int_ruca_codes", "county_fips", "09%")


class Pin(NamedTuple):
    """The delivered query one pinned capability is granted to: the fingerprint of its normalized parse (``query_fingerprint``) and why it is pinned."""

    fingerprint: str
    reason: str


PINNED_CAPABILITIES = frozenset({"label_bounds", "county_list_bounds", "edition_year", "state_prefix", "tract_count", "county_prefix"})
PINS: dict[tuple[str, str], Pin] = {
    ("dim_rurality_code", "label_bounds"): Pin(
        "54899dd39ef73441ccf035e8429a37647b7515629aa4df5e62cb0141e5ae50f7",
        "dim_rurality_code: the first and last vintage of each published label of a split RUCC code bound its edition and reach its key",
    ),
    ("dim_measure", "label_bounds"): Pin(
        "28bb15707754e643fed47961239a0aebc90204fc7ef44be45a3aae94a45813d5",
        "dim_measure: the first and last release date of each published name of a split measure bound its edition and reach its key and identity text",
    ),
    ("dim_county", "county_list_bounds"): Pin(
        "0bc525cb1c8d686beb9faeacb9a2d205c280f858be83b861a0a051dcfcc77f80",
        "dim_county: the county-list vintage bounds and the membership in the latest adjacency list rest on its chain of lists, latest vintage and latest list",
    ),
    ("bridge_measure_control", "edition_year"): Pin(
        "abc23441460b6840d1289fc2146800d4a1974f3a30d399ee978c548b4f6c6ce6",
        "bridge_measure_control: the edition years are compared with the seed range of the same seed row and the key hashes the stored edition start",
    ),
    ("dim_state", "state_prefix"): Pin(
        "6e7030a92311d15545dd24f39dc5375431d3117ac3ab5d3998ed50f87eb6bc81",
        "dim_state: the first two characters of the RUCC county code give the state code its FIPS prefix",
    ),
    ("bridge_county_planning_region", "tract_count"): Pin(
        "f5184ab1d2281da8581bf1d1cfaeb62270efbc7bb4861c52b4c38e9c3f92c496",
        "bridge_county_planning_region: the tract count is the number of RUCA tract rows of each old county, planning region and file",
    ),
    ("bridge_county_planning_region", "county_prefix"): Pin(
        "f5184ab1d2281da8581bf1d1cfaeb62270efbc7bb4861c52b4c38e9c3f92c496",
        "bridge_county_planning_region: the county prefix filter keeps the 2020 tract rows of the old Connecticut counties",
    ),
}
EDITION_BOUND = frozenset({"range_bound:min"})
LATEST_VINTAGE = "latest_vintage"
LATEST_LIST = "latest_list"
PROOF_LABELS = frozenset({LATEST_VINTAGE, LATEST_LIST})
LIST_NAME_COLUMN = "list_name"
CALENDAR_COLUMN = "calendar_date"
FISCAL_COLUMN = "federal_fiscal_year"
# Source and role bindings used when the contract omits an expression policy.
A36_A39_A43_A48: Json = {
    "date_roles": {
        "calendar_year": ["year"],
        "iso_week": ["week", "weekofyear"],
        "iso_year": ["isoyear"],
        "month": ["month"],
        "month_number": ["month"],
        "quarter": ["quarter"],
        "quarter_number": ["quarter"],
    },
    "label_choice": {"dim_drg": ["code_system", "code"], "dim_measure": ["fact", "measure_key"], "dim_rurality_code": ["code_system", "code"]},
    "normalization_inputs": {
        "dim_census_tract": {"census_tract_geoid": [["int_ruca_codes", "geography_id"], ["int_hpsa_components", "geography_id"]]},
        "dim_change_type": {"code": [["int_change_of_ownership_rows", "chow_type_code"]], "code_label": [["int_change_of_ownership_rows", "chow_type_text"]]},
        "dim_condition": {"code": [["int_mmd_prevalence", "condition_label"]], "code_label": [["int_mmd_prevalence", "condition_label"]]},
        "dim_county": {
            "county_fips": [
                ["int_acs_county_values", "county_fips"],
                ["int_bls_county_series", "county_fips"],
                ["int_county_adjacency_edges", "county_fips"],
                ["int_county_adjacency_edges", "neighbor_fips"],
                ["int_gv_county_values", "county_fips"],
                ["int_hpsa_components", "county_fips"],
                ["int_mmd_prevalence", "county_fips"],
                ["int_mua_components", "county_fips"],
                ["int_places_county_values", "county_fips"],
                ["int_pos_hospital_snapshots", "county_fips"],
                ["int_ruca_codes", "county_fips"],
                ["int_rucc_county_codes", "county_fips"],
                ["int_sahie_county_rows", "county_fips"],
                ["int_saipe_county_estimates", "county_fips"],
                ["int_svi_county_values", "county_fips"],
                ["int_wonder_county_deaths", "county_fips"],
            ]
        },
        "dim_demographic_group": {
            "code": [
                ["int_sahie_county_rows", "agecat"],
                ["int_sahie_county_rows", "racecat"],
                ["int_sahie_county_rows", "sexcat"],
                ["int_sahie_county_rows", "iprcat"],
                ["int_gv_county_values", "bene_age_lvl"],
            ]
        },
        "dim_drg": {
            "@order": [["int_mup_drg_discharges", "data_year"]],
            "code": [["int_mup_drg_discharges", "drg_cd"]],
            "code_label": [["int_mup_drg_discharges", "drg_desc"]],
        },
        "dim_ehr_product": {"code": [["int_onc_chpl_linkage_rows", "product_database_id"]], "code_label": [["int_onc_chpl_linkage_rows", "product_name"]]},
        "dim_enrollment": {
            "enrollment_id": [
                ["int_change_of_ownership_rows", "enrollment_id_buyer"],
                ["int_change_of_ownership_rows", "enrollment_id_seller"],
                ["int_hospital_enrollment_rows", "enrollment_id"],
                ["int_hospital_owner_rows", "enrollment_id"],
            ]
        },
        "dim_footnote": {
            "code": [
                ["int_cc_complications_deaths_windows", "footnote"],
                ["int_cc_hcahps_windows", "hcahps_answer_percent_footnote"],
                ["int_cc_hcahps_windows", "number_of_completed_surveys_footnote"],
                ["int_cc_hcahps_windows", "patient_survey_star_rating_footnote"],
                ["int_cc_hcahps_windows", "survey_response_rate_percent_footnote"],
                ["int_cc_hrrp_windows", "footnote"],
                ["int_cc_maternal_windows", "footnote"],
                ["int_cc_timely_effective_windows", "footnote"],
                ["int_cc_unplanned_visits_windows", "footnote"],
                ["int_hac_program_years", "payment_reduction_footnote"],
                ["int_hac_program_years", "total_hac_score_footnote"],
                ["int_hgi_hospital_releases", "overall_rating_footnote"],
            ]
        },
        "dim_hospital": {
            "ccn": [
                ["int_cc_complications_deaths_windows", "entity_id"],
                ["int_cc_hcahps_windows", "entity_id"],
                ["int_cc_hrrp_windows", "entity_id"],
                ["int_cc_maternal_windows", "entity_id"],
                ["int_cc_timely_effective_windows", "entity_id"],
                ["int_cc_unplanned_visits_windows", "entity_id"],
                ["int_cc_window_holds", "entity_id"],
                ["int_change_of_ownership_rows", "ccn_buyer"],
                ["int_change_of_ownership_rows", "ccn_seller"],
                ["int_cmi_hospital_rows", "ccn"],
                ["int_cost_reports", "ccn"],
                ["int_hac_program_years", "ccn"],
                ["int_hai_hospital_windows", "entity_id"],
                ["int_hai_window_holds", "entity_id"],
                ["int_hgi_hospital_releases", "ccn"],
                ["int_hhs_capacity_weeks", "ccn"],
                ["int_hospital_enrollment_rows", "ccn"],
                ["int_impact_holds", "ccn"],
                ["int_impact_hospital_values", "ccn"],
                ["int_mup_drg_discharges", "ccn"],
                ["int_mup_providers", "ccn"],
                ["int_occmix_survey_rows", "ccn"],
                ["int_onc_attestation_rows", "ccn"],
                ["int_onc_chpl_linkage_rows", "ccn"],
                ["int_pos_hospital_snapshots", "ccn"],
                ["int_validation_program_holds", "ccn"],
                ["int_validation_window_holds", "entity_id"],
                ["int_vbp_program_years", "ccn"],
            ]
        },
        "dim_measure": {
            "measure_key": [
                ["int_acs_county_values", "concept_id"],
                ["int_bls_county_series", "measure"],
                ["int_cc_complications_deaths_windows", "measure_id"],
                ["int_gv_county_values", "field"],
                ["int_hai_hospital_windows", "measure_id"],
                ["int_hai_national_windows", "measure_id"],
                ["int_hai_state_windows", "measure_id"],
                ["int_cc_hcahps_windows", "measure_id"],
                ["int_cc_hrrp_windows", "measure_id"],
                ["int_impact_hospital_values", "field"],
                ["int_cc_maternal_windows", "measure_id"],
                ["int_mup_drg_discharges", "drg_cd"],
                ["int_mmd_prevalence", "condition_label"],
                ["int_places_county_values", "measureid"],
                ["int_svi_county_values", "field"],
                ["int_cc_timely_effective_windows", "measure_id"],
                ["int_cc_unplanned_visits_windows", "measure_id"],
            ]
        },
        "dim_owner_organization": {
            "associate_id": [
                ["int_change_of_ownership_rows", "associate_id_buyer"],
                ["int_change_of_ownership_rows", "associate_id_seller"],
                ["int_hospital_owner_rows", "associate_id"],
            ]
        },
        "dim_owner_role": {"code": [["int_hospital_owner_rows", "role_code"]], "code_label": [["int_hospital_owner_rows", "role_text"]]},
        "dim_rurality_code": {
            "@order": [["int_rucc_county_codes", "vintage"]],
            "code": [["int_rucc_county_codes", "rucc_code"], ["int_ruca_codes", "primary_ruca"], ["int_pos_hospital_snapshots", "cbsa_urban_rural_code"]],
            "code_label": [["int_rucc_county_codes", "description"]],
        },
        "dim_source": {"dataset_id": [["stg_bronze__release_observations", "dataset_id"]]},
        "dim_state": {
            "state_code": [
                ["int_cost_reports", "state_code"],
                ["int_hai_state_windows", "entity_id"],
                ["int_hgi_hospital_releases", "state_code"],
                ["int_hhs_capacity_weeks", "state"],
                ["int_hospital_enrollment_rows", "state"],
                ["int_mup_providers", "state_code"],
                ["int_onc_chpl_linkage_rows", "state"],
                ["int_pos_hospital_snapshots", "state_code"],
                ["int_rucc_county_codes", "state"],
            ]
        },
        "dim_zip": {
            "zip_code": [
                ["int_cost_reports", "zip_code"],
                ["int_hgi_hospital_releases", "zip_code"],
                ["int_hhs_capacity_weeks", "zip"],
                ["int_hospital_enrollment_rows", "zip_code"],
                ["int_hsa_zip_cases", "zip_code"],
                ["int_hud_zip_county_holds", "zip_code"],
                ["int_onc_attestation_rows", "zip"],
                ["int_onc_chpl_linkage_rows", "zip_code"],
                ["int_pos_hospital_snapshots", "zip_code"],
            ]
        },
    },
    "release": {"model": "dim_release", "object_column": "object_key", "publication_column": "publication_release", "relation": "stg_bronze__file_copies"},
    "source_date_models": ["dim_date", "dim_hospital"],
    "value_space": {
        "dim_census_tract": ["regexp_matches", "census_tract_geoid", "^[0-9]{11}$"],
        "dim_county": ["length", "county_fips", 5],
        "dim_hospital": ["length", "ccn", 6],
        "dim_owner_organization": ["regexp_matches", "associate_id", "^[0-9]{10}$"],
        "dim_state": ["regexp_matches", "state_code", "^[A-Z]{2}$"],
        "dim_zip": ["regexp_matches", "zip_code", "^[0-9]{5}$"],
    },
}


class PolicyError(ValueError):
    """The contract's allowlist holds an entry this check does not know, or a dimension has no natural-key specification."""


def capabilities(allowlist: tuple[str, ...]) -> frozenset[str]:
    """The capabilities the contract allowlist switches on; an unknown entry is an error, not a silent grant."""
    unknown = [entry for entry in allowlist if entry not in CAPABILITIES]
    if unknown:
        raise PolicyError(f"expression_allowlist entries this check cannot read: {unknown}")
    return frozenset(CAPABILITIES[entry] for entry in allowlist)


@dataclass(frozen=True)
class Policy:
    """Everything the expression policy reads from the contract and from DuckDB."""

    capabilities: frozenset[str]
    value_columns: frozenset[str]
    date_pairs: frozenset[Base]
    aggregates: frozenset[str]
    scopes: Json = field(default_factory=lambda: A36_A39_A43_A48)
    pins: dict[tuple[str, str], Pin] = field(default_factory=lambda: PINS)


@dataclass(frozen=True)
class Finding:
    """One violation: ``check`` is arithmetic, case, functions or lineage."""

    check: str
    message: str


@dataclass(frozen=True)
class Occurrence:
    """A column read, with each FROM reference on its path kept distinct from physical lineage."""

    path: tuple[int, ...]
    column: str


@dataclass(frozen=True)
class Selection:
    """A complete label selection or exclusion, bound to its input scope and projected key values."""

    scope: int
    keys: tuple[str, ...]
    inputs: tuple[frozenset[Occurrence], ...]
    label: frozenset[Occurrence]
    literals: tuple[frozenset[str], ...]
    domain: int


@dataclass(frozen=True)
class Flow:
    occurrences: frozenset[Occurrence] = frozenset()
    selections: frozenset[Selection] = frozenset()
    exclusions: frozenset[Selection] = frozenset()
    null: bool = False
    literals: frozenset[str] = frozenset()


class StarSource(NamedTuple):
    relation: str
    path: tuple[int, ...]


@dataclass(frozen=True)
class Col:
    """An output column: its name, the (relation, column) bases it derives from and what kind of expression made it."""

    name: str
    bases: frozenset[Base]
    labels: frozenset[str]
    inputs: tuple[Value, ...] = ()
    flow: Flow = Flow()


@dataclass
class Out:
    """The output of a query: its columns and the physical relations whose unnamed columns (a star) also flow through."""

    columns: list[Col]
    star_relations: tuple[StarSource, ...] = ()

    def get(self, name: str) -> Col | None:
        return next((column for column in self.columns if column.name == name), None)


@dataclass(frozen=True)
class Value:
    bases: frozenset[Base]
    labels: frozenset[str]
    inputs: tuple[Value, ...] = ()
    flow: Flow = Flow()


@dataclass
class Source:
    """A relation in a FROM clause; ``out`` is None for a physical relation (its columns are not known)."""

    alias: str
    out: Out | None
    relation: str = "?"
    occurrence: int = 0


@dataclass
class Scope:
    sources: list[Source]
    ctes: dict[str, Out]
    outer: Scope | None = None
    projected: Out | None = None
    identity: int = 0
    using: dict[str, str] = field(default_factory=dict)
    equalities: list[tuple[Flow, Flow]] = field(default_factory=list)
    value_equalities: list[tuple[Value, Value]] = field(default_factory=list)
    # A FROM reference with TABLESAMPLE restricts the rows it reads, like a WHERE clause.
    sampled: bool = False
    groups: tuple[Json, ...] = ()
    left_joins: list[tuple[Flow, Flow]] = field(default_factory=list)


@dataclass
class Analysis:
    findings: list[Finding]
    reads: set[str]
    top: Out


COLUMN = frozenset({"column"})
CONSTANT = frozenset({"const"})
OTHER = frozenset({"other"})
EMPTY: frozenset[Base] = frozenset()


def key_columns(spec: KeySpec, count: int) -> tuple[str, ...]:
    """The columns a key of ``count`` parts hashes: the natural key, and the edition when the key has one part more."""
    return (*spec.columns, spec.edition) if spec.edition and count == len(spec.columns) + 1 else spec.columns


def dimension_key(dimension: str) -> str:
    """A dimension's surrogate-key column: its name without ``dim_`` plus ``_sk``."""
    return dimension.removeprefix("dim_") + "_sk"


def column_names(bases: frozenset[Base]) -> set[str]:
    return {column for _relation, column in bases}


def no_edition(column: Col | Value) -> bool:
    """A typed null edition: the row has none."""
    return column.flow.null and not column.bases


def bounded_edition(column: Col | Value) -> bool:
    """An edition that is the ``min`` of a date and is never null."""
    return bool(column.labels & EDITION_BOUND) and not column.flow.null


def merged_labels(left: Col, right: Col) -> frozenset[str]:
    """The labels of a union column; a proof label (the newest vintage, the latest list) survives only when both arms hold it."""
    return (left.labels | right.labels) - PROOF_LABELS | (PROOF_LABELS & left.labels & right.labels)


def plain_call(node: Json) -> bool:
    """An aggregate call with no filter, distinct or order by: only its arguments decide its value."""
    return node.get("filter") is None and not node.get("distinct") and not node["order_bys"]["orders"] and not node.get("export_state")


def plain_grouping(node: Json) -> tuple[Json, ...]:
    """The columns of a GROUP BY of plain columns in one grouping set; empty when there is none or it is anything else."""
    groups = node.get("group_expressions", [])
    if not groups or not all(is_column(group) for group in groups) or node.get("group_sets") != [list(range(len(groups)))]:
        return ()
    return tuple(groups)


def simple_select(node: Json) -> bool:
    """A select with no GROUP BY, HAVING, QUALIFY, modifier or sample: its WHERE clause alone narrows its rows."""
    return not any(node.get(key) for key in ("group_expressions", "having", "qualify", "modifiers", "sample"))


def disjuncts(node: Json) -> list[Json]:
    """The operands of an OR, through nested ORs."""
    if node["class"] == "CONJUNCTION" and node["type"] == "CONJUNCTION_OR":
        return [part for child in node["children"] for part in disjuncts(child)]
    return [node]


def item_calls(node: Json) -> set[int]:
    """A select item that is a call, or a plain cast of a call: the calls whose result becomes the output column."""
    calls = {id(node)}
    if node["class"] == "CAST" and not node["try_cast"] and node["child"]["class"] == "FUNCTION":
        calls.add(id(node["child"]))
    return calls


def from_source(value: Value | Col, source: Source) -> bool:
    """Every occurrence of the value is a column of that FROM source."""
    return bool(value.flow.occurrences) and all(origin.path[0] == source.occurrence for origin in value.flow.occurrences)


def is_column(node: Json | None) -> bool:
    return node is not None and node["class"] == "COLUMN_REF"


def column_name(node: Json | None) -> str | None:
    return str(node["column_names"][-1]) if is_column(node) and node is not None else None


def is_text(node: Json | None, text: str | None = None) -> bool:
    """A VARCHAR constant, equal to ``text`` when one is given."""
    if node is None or node["class"] != "CONSTANT" or node["value"]["is_null"] or node["value"]["type"]["id"] != "VARCHAR":
        return False
    return text is None or node["value"]["value"] == text


def constant_cast(node: Json) -> bool:
    """A constant, or a cast of one (a typed null)."""
    return node["class"] == "CONSTANT" or (node["class"] == "CAST" and not node["try_cast"] and constant_cast(node["child"]))


def is_unknown_hash(node: Json | None) -> bool:
    return (
        node is not None
        and node["class"] == "FUNCTION"
        and node["function_name"] == "md5"
        and len(node["children"]) == 1
        and is_text(node["children"][0], UNKNOWN)
    )


def innermost_constant(node: Json) -> Any:
    """The number inside an interval built by ``to_days(cast(trunc(cast(3 as double)) as integer))``, or None for another shape."""
    if node["class"] == "CONSTANT":
        return node["value"]["value"]
    if node["class"] == "CAST" and not node["try_cast"]:
        return innermost_constant(node["child"])
    if node["class"] == "FUNCTION" and node["function_name"] == "trunc" and len(node["children"]) == 1:
        return innermost_constant(node["children"][0])
    return None


def interval(node: Json | None, unit: str, amount: int) -> bool:
    """``interval <amount> <unit>`` as DuckDB parses it: ``to_days`` or ``to_months`` of a constant."""
    if node is None or node["class"] != "FUNCTION" or node["function_name"] != unit or len(node["children"]) != 1:
        return False
    return innermost_constant(node["children"][0]) == amount


def merge_inputs(left: tuple[Value, ...], right: tuple[Value, ...]) -> tuple[Value, ...]:
    """Merge ordered operands after each set arm has independently bound its key to its natural outputs."""
    if not left or not right:
        return left or right
    if len(left) != len(right):
        return ()
    return tuple(
        Value(
            a.bases | b.bases,
            a.labels | b.labels,
            flow=Flow(
                a.flow.occurrences | b.flow.occurrences,
                null=a.flow.null and b.flow.null,
                literals=a.flow.literals | b.flow.literals if a.flow.literals and b.flow.literals else frozenset(),
            ),
        )
        for a, b in zip(left, right, strict=True)
    )


def same_value(left: Value | Col, right: Value | Col) -> bool:
    """Physical equality alone cannot equate separate reads of the same column."""
    return (
        left.bases == right.bases
        and left.flow.occurrences == right.flow.occurrences
        and left.flow.literals == right.flow.literals
        and left.flow.null == right.flow.null
    )


def prefix_occurrences(values: frozenset[Occurrence], occurrence: int) -> frozenset[Occurrence]:
    return frozenset(Occurrence((occurrence, *value.path), value.column) for value in values)


def reference_flow(flow: Flow, occurrence: int) -> Flow:
    def rebound(proofs: frozenset[Selection]) -> frozenset[Selection]:
        return frozenset(
            replace(proof, inputs=tuple(prefix_occurrences(part, occurrence) for part in proof.inputs), label=prefix_occurrences(proof.label, occurrence))
            for proof in proofs
        )

    origins = prefix_occurrences(flow.occurrences, occurrence)
    if not origins and (flow.literals or flow.null):
        origins = frozenset({Occurrence((occurrence,), "$literal:" + repr(tuple(sorted(flow.literals))) if flow.literals else "$null")})
    return replace(flow, occurrences=origins, selections=rebound(flow.selections), exclusions=rebound(flow.exclusions))


def proof_matches(proof: Selection, out: Out, label: Col) -> bool:
    columns = [out.get(key) for key in proof.keys]
    return (
        proof.label == label.flow.occurrences
        and all(column is not None for column in columns)
        and tuple(column.flow.occurrences for column in columns if column is not None) == proof.inputs
        and tuple(column.flow.literals for column in columns if column is not None) == proof.literals
    )


def conjunctions(node: Json) -> list[Json]:
    if node["class"] == "CONJUNCTION" and node["type"] == "CONJUNCTION_AND":
        return [part for child in node["children"] for part in conjunctions(child)]
    return [node]


def child_nodes(node: Any) -> list[Json]:
    """The expression nodes directly under a node, found through any containers between them (so no field is skipped)."""
    found: list[Json] = []
    values = list(node.values()) if isinstance(node, dict) else list(node) if isinstance(node, list) else []
    for value in values:
        if isinstance(value, dict):
            if "class" in value:
                found.append(value)
            elif value.get("type") not in QUERY_NODES:
                found += child_nodes(value)
        elif isinstance(value, list):
            found += child_nodes(value)
    return found


class Walker:
    """One pass over a model's parse: collects findings and physical reads, and resolves lineage."""

    def __init__(self, model: str, kind: str, policy: Policy, fact_map: dict[str, tuple[str, str]], fingerprint: str) -> None:
        self.model = model
        self.kind = kind
        self.policy = policy
        self.fact_map = fact_map
        self.fingerprint = fingerprint
        self.joined: dict[str, tuple[str, str]] = {}
        self.findings: list[Finding] = []
        self.reads: set[str] = set()
        self.seen: set[int] = set()
        self.normalizations: dict[str, set[str]] = {}
        self.used_normalizations: set[str] = set()
        self.value_filters: list[tuple[str, Value]] = []
        self.serial = 0
        self.edition_parts: set[int] = set()
        self.year_calls: set[int] = set()
        self.year_slots: set[int] = set()
        self.projected_calls: set[int] = set()
        self.where_conjuncts: set[int] = set()
        self.membership_selects: set[int] = set()
        self.clause = ""

    def granted(self, capability: str) -> bool:
        """The contract allowlist switches the capability on and, for a pinned capability, this model's whole query is the pinned query."""
        if capability not in self.policy.capabilities:
            return False
        if capability not in PINNED_CAPABILITIES:
            return True
        pin = self.policy.pins.get((self.model, capability))
        return pin is not None and pin.fingerprint == self.fingerprint

    def pin_findings(self) -> None:
        """A model with pinned capabilities that is not its pinned query: the finding names both fingerprints and the reason."""
        for (model, capability), pin in sorted(self.policy.pins.items()):
            if model == self.model and capability in self.policy.capabilities and pin.fingerprint != self.fingerprint:
                self.flag(
                    "functions",
                    f"{capability} is pinned to the delivered {model} query ({pin.fingerprint}); this query is {self.fingerprint}: {pin.reason}; "
                    "change the pin and its probes deliberately",
                )

    def identity_number(self) -> int:
        self.serial += 1
        return self.serial

    def source(self, alias: str, out: Out | None, relation: str = "?") -> Source:
        """Instantiate a FROM item; every CTE reference receives its own occurrence path."""
        occurrence = self.identity_number()
        if out is not None:
            out = Out(
                [
                    replace(
                        column,
                        inputs=tuple(replace(part, flow=reference_flow(part.flow, occurrence)) for part in column.inputs),
                        flow=reference_flow(column.flow, occurrence),
                    )
                    for column in out.columns
                ],
                tuple(StarSource(star.relation, (occurrence, *star.path)) for star in out.star_relations),
            )
        return Source(alias, out, relation, occurrence)

    def certificate(self, identity: int, out: Out, column: Col, domain: int | None = None) -> Selection:
        keys = tuple(self.policy.scopes["label_choice"][self.model])
        values = [out.get(key) for key in keys]
        return Selection(
            identity,
            keys,
            tuple(value.flow.occurrences if value else frozenset() for value in values),
            column.flow.occurrences,
            tuple(value.flow.literals if value else frozenset() for value in values),
            identity if domain is None else domain,
        )

    def disjoint_labels(self, left: Out, right: Out, a: Col, b: Col) -> bool:
        """Prove the reserved unknown row, constant key domains or a full-key anti-selection."""
        keys = self.policy.scopes["label_choice"].get(self.model, [])
        if not keys:
            return False
        if self.edition_disjoint(left, right):
            return True
        if any(
            (x := left.get(key)) is not None
            and (y := right.get(key)) is not None
            and x.flow.literals
            and y.flow.literals
            and x.flow.literals.isdisjoint(y.flow.literals)
            for key in keys
        ):
            return True
        for selected, other, label, other_label in ((left, right, a, b), (right, left, b, a)):
            unknown_key = other.get(dimension_key(self.model))
            unknown_values = all((value := other.get(key)) is not None and value.flow.literals == {UNKNOWN} and not value.bases for key in keys)
            measure_unknown = self.model == "dim_measure" and all((value := other.get(key)) is not None and value.flow.null for key in keys)
            if (
                other_label.flow.null
                and not other_label.bases
                and unknown_key is not None
                and unknown_key.flow.literals == {UNKNOWN}
                and (unknown_values or measure_unknown)
            ):
                return True
            chosen = {proof.domain for proof in label.flow.selections if proof_matches(proof, selected, label)}
            excluded = {proof.domain for proof in other_label.flow.exclusions if proof_matches(proof, other, other_label)}
            if chosen and chosen <= excluded:
                return True
        return False

    def edition_disjoint(self, left: Out, right: Out) -> bool:
        """Rows with a null edition and rows with a bounded edition never share a key, because the edition is the last part of the key."""
        bound = RANGE_BOUNDS.get(self.model)
        if bound is None or bound.capability != "label_bounds" or not self.granted(bound.capability):
            return False
        a, b = left.get(bound.minimum), right.get(bound.minimum)
        if a is None or b is None:
            return False
        return (no_edition(a) and bounded_edition(b)) or (bounded_edition(a) and no_edition(b))

    def set_output(self, node: Json, left: Out, right: Out) -> Out:
        """UNION invalidates overlapping selections; subtracting or intersecting rows keeps only left-side evidence."""
        # The reserved unknown hash has no source operand. Use that arm's natural values when merging its explicit sentinel row.
        for arm in (left, right):
            for index, column in enumerate(arm.columns):
                spec = DIMENSION_KEYS.get(self.model) if column.name == dimension_key(self.model) else BRIDGE_KEYS.get(self.model, {}).get(column.name)
                if spec is not None and not column.inputs and column.flow.literals == {UNKNOWN} and "md5_key" in column.labels:
                    carried = arm.get(spec.edition) if spec.edition else None
                    names = (*spec.columns, spec.edition) if carried is not None and no_edition(carried) else spec.columns
                    natural = [arm.get(name) for name in names]
                    null_allowed = self.model in NULL_SENTINEL_MODELS
                    if all(
                        value is not None
                        and not value.bases
                        and ((null_allowed or name == spec.edition) and value.flow.null or value.flow.literals == {UNKNOWN})
                        for name, value in zip(names, natural, strict=True)
                    ):
                        arm.columns[index] = replace(column, inputs=tuple(Value(value.bases, value.labels, flow=value.flow) for value in natural if value))
        if node["setop_type"] in {"EXCEPT", "INTERSECT"}:
            identity = self.identity_number()
            return Out(
                [
                    replace(column, flow=replace(column.flow, selections=frozenset(replace(proof, domain=identity) for proof in column.flow.selections)))
                    for column in left.columns
                ],
                left.star_relations,
            )
        if node["setop_type"] != "UNION":
            self.flag("functions", f"set operation {node['setop_type']} is not supported")
        # Tag each union arm in every output and operand. Ordered identities remain correlated even through renamed literal-only arms.
        left_source, right_source = self.source("", left), self.source("", right)
        if left_source.out is None or right_source.out is None:
            raise PolicyError("set-operation outputs are unavailable")
        left, right = left_source.out, right_source.out
        identity = self.identity_number()
        columns = [
            Col(
                a.name,
                a.bases | b.bases,
                merged_labels(a, b),
                merge_inputs(a.inputs, b.inputs),
                Flow(
                    a.flow.occurrences | b.flow.occurrences,
                    null=a.flow.null and b.flow.null,
                    literals=a.flow.literals | b.flow.literals if a.flow.literals and b.flow.literals else frozenset(),
                ),
            )
            for a, b in zip(left.columns, right.columns, strict=False)
        ]
        out = Out(columns, tuple(dict.fromkeys(left.star_relations + right.star_relations)))
        for index, (a, b) in enumerate(zip(left.columns, right.columns, strict=False)):
            if "label_pending" not in columns[index].labels:
                continue
            selected_a = "label_pending" not in a.labels or any(proof_matches(proof, left, a) for proof in a.flow.selections)
            selected_b = "label_pending" not in b.labels or any(proof_matches(proof, right, b) for proof in b.flow.selections)
            if selected_a and selected_b and self.disjoint_labels(left, right, a, b):
                column = columns[index]
                columns[index] = replace(column, flow=replace(column.flow, selections=frozenset({self.certificate(identity, out, column)})))
        return out

    def flag(self, check: str, message: str) -> None:
        self.findings.append(Finding(check, f"{self.model}: {message}"))

    def mark(self, node: Any) -> None:
        """Record that a whole subtree was recognized by a role."""
        if isinstance(node, dict):
            self.seen.add(id(node))
            for value in node.values():
                self.mark(value)
        elif isinstance(node, list):
            for value in node:
                self.mark(value)

    def sweep(self, node: Any) -> None:
        """Report any expression node that no handler visited and no role recognized, and any call outside the unqualified built-in names."""
        if isinstance(node, dict):
            if node.get("class") in ("FUNCTION", "WINDOW") and (node.get("schema") or node.get("catalog")):
                qualified = ".".join(part for part in (node["catalog"], node["schema"], node["function_name"]) if part)
                self.flag("functions", f"function {qualified} is qualified; only the unqualified built-in name is approved")
            if "class" in node and id(node) not in self.seen:
                self.flag("functions", f"expression {node['class']} was not analyzed (unsupported shape)")
                return
            for value in node.values():
                self.sweep(value)
        elif isinstance(node, list):
            for value in node:
                self.sweep(value)

    # Queries and relations.
    def renamed_output(self, out: Out, aliases: list[str]) -> Out:
        if not aliases:
            return out
        if out.star_relations or len(aliases) > len(out.columns):
            self.flag("functions", "column alias list was not analyzed (unsupported shape): unnamed source columns")
        return Out([replace(column, name=aliases[index]) if index < len(aliases) else column for index, column in enumerate(out.columns)], out.star_relations)

    def query(self, node: Json, ctes: dict[str, Out], outer: Scope | None = None) -> Out:
        local = dict(ctes)
        for entry in (node.get("cte_map") or {}).get("map", []):
            out = self.query(entry["value"]["query"]["node"], local, outer)
            aliases = entry["value"].get("aliases") or []
            local[entry["key"]] = self.renamed_output(out, aliases)
        if node["type"] == "SELECT_NODE":
            out = self.select(node, local, outer)
        elif node["type"] == "SET_OPERATION_NODE":
            left, right = self.query(node["left"], local, outer), self.query(node["right"], local, outer)
            out = self.set_output(node, left, right)
        else:
            self.flag("functions", f"query node {node['type']} is not supported")
            return Out([], (StarSource("?", (self.identity_number(),)),))
        if node["type"] != "SELECT_NODE":
            for modifier in node.get("modifiers", []):
                self.modifier(modifier, Scope([Source("", out)], local))
        return out

    def modifier(self, modifier: Json, scope: Scope) -> None:
        targets = modifier.get("distinct_on_targets", [])
        label_choice = self.policy.scopes["label_choice"].get(self.model, [])
        approved_choice = bool(label_choice) and [column_name(target) for target in targets] == label_choice
        for target in targets:
            value = self.expr(target, scope, "predicate")
            if approved_choice and is_column(target):
                self.use_normalizations(value, column_name(target) or "")
        for order in modifier.get("orders", []):
            value = self.expr(order["expression"], scope, "predicate")
            self.use_normalizations(value, "@order") if "normalize:@order" in value.labels else None
            if "normalize:@order" in value.labels and (order["type"] != "DESCENDING" or order["null_order"] != "NULLS LAST"):
                self.flag("functions", "A48 ordering cast requires descending order with nulls last")
        for key in ("limit", "offset"):
            if modifier.get(key):
                self.expr(modifier[key], scope, "predicate")

    def label_selection(self, node: Json, scope: Scope, out: Out) -> None:
        """Label selection requires the keys, source vintages and first label in one selecting scope."""
        modifiers = node.get("modifiers", [])
        targets = [target for modifier in modifiers for target in modifier.get("distinct_on_targets", [])]
        if not targets:
            return
        keys = self.policy.scopes["label_choice"].get(self.model, [])
        valid = bool(keys) and [column_name(target) for target in targets] == keys
        rules = self.policy.scopes["normalization_inputs"].get(self.model, {})
        label = "measure_name" if self.model == "dim_measure" else "code_label"
        selected_label = out.get(label)
        label_paths = {origin.path for origin in selected_label.flow.occurrences} if selected_label else set()
        for target, key in zip(targets, keys, strict=False):
            value = self.column(target, scope) if is_column(target) else self.expr(target, scope, "predicate")
            selected = out.get(key)
            allowed = {tuple(pair) for pair in rules.get(key, [])}
            valid &= selected is not None and same_value(value, selected)
            if value.flow.occurrences:
                valid &= label_paths <= {origin.path for origin in value.flow.occurrences}
            if allowed:
                valid &= bool(value.bases) and value.bases <= allowed
        vintages = ["release_date", "window_end"] if self.model == "dim_measure" else ["label_order"]
        expected = [*keys, *vintages, label]
        orders = [order for modifier in modifiers for order in modifier.get("orders", [])]
        valid &= len(orders) == len(expected)
        for order, name in zip(orders, expected, strict=False):
            expression = order["expression"]
            value = self.column(expression, scope) if is_column(expression) else self.expr(expression, scope, "predicate")
            valid &= column_name(expression) == name
            descending = name in vintages
            valid &= order["type"] == "DESCENDING" if descending else order["type"] in {"ASCENDING", "ORDER_DEFAULT"}
            valid &= order["null_order"] == "NULLS LAST" if descending else order["null_order"] in {"NULLS LAST", "ORDER_DEFAULT"}
            if descending:
                valid &= label_paths <= {origin.path for origin in value.flow.occurrences}
                if self.model == "dim_measure":
                    allowed = {(relation, name) for relation, _column in rules.get("measure_key", [])}
                else:
                    allowed = {tuple(pair) for pair in rules.get("@order", [])}
                valid &= bool(value.bases) and value.bases <= allowed
            else:
                selected = out.get(name)
                valid &= selected is not None and same_value(value, selected)
                if name == label:
                    allowed = (
                        {(relation, "measure_name") for relation, _column in rules.get("measure_key", [])}
                        if self.model == "dim_measure"
                        else {tuple(pair) for pair in rules.get("code_label", [])}
                    )
                    valid &= bool(value.bases) and value.bases <= allowed
        if not valid:
            self.flag("functions", "A48 requires exact resolved DISTINCT ON keys and latest-vintage ordering with ascending label tie break")
            return
        out.columns = [
            replace(column, flow=replace(column.flow, selections=frozenset({self.certificate(scope.identity, out, column)})))
            if column.name == label
            else column
            for column in out.columns
        ]

    def select(self, node: Json, ctes: dict[str, Out], outer: Scope | None = None) -> Out:
        enclosing, self.clause = self.clause, ""
        scope = Scope([], ctes, outer, identity=self.identity_number(), groups=plain_grouping(node))
        self.relations(node["from_table"], scope)
        columns: list[Col] = []
        stars: list[StarSource] = []
        for item in node["select_list"]:
            found, relations = self.item(item, scope)
            columns += found
            stars += relations
        scope.projected = Out(columns, tuple(dict.fromkeys(stars)))
        if node.get("where_clause"):
            self.where(node["where_clause"], scope)
        for clause in ("having", "qualify"):
            if node.get(clause):
                self.expr(node[clause], scope, "predicate")
        for group in node.get("group_expressions", []):
            self.expr(group, scope, "predicate")
        out = Out(columns, tuple(dict.fromkeys(stars)))
        self.project_hash_inputs(out, scope)
        self.project_proofs(out, scope, node)
        modifier_scope = Scope([Source("", out), *scope.sources], ctes, outer, identity=scope.identity, using=scope.using)
        self.label_selection(node, modifier_scope, out)
        self.edition_selection(node, scope, out)
        self.county_list_proofs(node, scope, out)
        for modifier in node.get("modifiers", []):
            self.modifier(modifier, modifier_scope)
        self.clause = enclosing
        return out

    def edition_selection(self, node: Json, scope: Scope, out: Out) -> None:
        """The label selection with editions: one row per published label of a code, grouped by the keys and the label, bounded by ``min`` and ``max``.

        The label is selected when the select groups by exactly the label keys and the label, outputs the edition bounds of the model's allowlist entry
        over one date that the select filters as not null (so no edition takes the null edition of the single-label rows), and so the label travels
        with an edition in the key. Anything else leaves the label unselected.
        """
        bound = RANGE_BOUNDS.get(self.model)
        keys = self.policy.scopes["label_choice"].get(self.model, [])
        if bound is None or bound.capability != "label_bounds" or not self.granted(bound.capability) or not keys:
            return
        labels = [column for column in out.columns if "label_pending" in column.labels]
        first, last = out.get(bound.minimum), out.get(bound.maximum)
        if len(labels) != 1 or first is None or last is None or "range_bound:min" not in first.labels or "range_bound:max" not in last.labels:
            return
        if not same_value(first, last) or not self.not_null_filter(node, scope, first):
            return
        grouped = [self.column(group, scope) if is_column(group) else None for group in node.get("group_expressions", [])]
        wanted = [out.get(key) for key in keys] + labels
        if len(grouped) != len(wanted) or any(value is None for value in grouped) or any(column is None for column in wanted):
            return
        if not all(any(column is not None and same_value(column, value) for value in grouped if value is not None) for column in wanted):
            return
        certificate = self.certificate(scope.identity, out, labels[0])
        out.columns = [
            replace(column, flow=replace(column.flow, selections=frozenset({certificate}))) if column is labels[0] else column for column in out.columns
        ]

    def not_null_filter(self, node: Json, scope: Scope, bounded: Col) -> bool:
        """The WHERE clause holds ``<the bounded date> is not null`` as one of its conjunctions."""
        return bool(node.get("where_clause")) and any(
            part["class"] == "OPERATOR"
            and part["type"] == "OPERATOR_IS_NOT_NULL"
            and len(part["children"]) == 1
            and is_column(part["children"][0])
            and (value := self.column(part["children"][0], scope)).bases == bounded.bases
            and value.flow.occurrences == bounded.flow.occurrences
            for part in conjunctions(node["where_clause"])
        )

    def project_hash_inputs(self, out: Out, scope: Scope) -> None:
        """Rebind hash operands only through proved inner-join value equalities to the corresponding natural output."""
        for index, column in enumerate(out.columns):
            spec = DIMENSION_KEYS.get(self.model) if column.name == dimension_key(self.model) else BRIDGE_KEYS.get(self.model, {}).get(column.name)
            identity = IDENTITY_OUTPUTS.get(self.model)
            if spec is None and identity and column.name == identity[0]:
                self.padded_edition(column, identity[1], out)
            if spec is None:
                continue
            padding = self.padded_edition(column, spec, out)
            inputs = (*column.inputs, padding) if padding is not None else column.inputs
            names = key_columns(spec, len(inputs))
            if len(inputs) != len(names):
                continue
            parts: list[Value] = []
            for name, hashed in zip(names, inputs, strict=True):
                natural = out.get(name)
                reachable = {hashed}
                while True:
                    expanded = reachable | {b for a, b in scope.value_equalities if any(same_value(a, value) for value in reachable)}
                    if expanded == reachable:
                        break
                    reachable = expanded
                if natural is not None and any(same_value(natural, value) for value in reachable):
                    hashed = Value(natural.bases, natural.labels, flow=natural.flow)
                parts.append(hashed)
            out.columns[index] = replace(column, inputs=tuple(parts))

    def padded_edition(self, column: Col, spec: KeySpec, out: Out) -> Value | None:
        """The typed null edition that completes a key leaving out its edition part; a key that omits an edition its row carries is a finding."""
        if not spec.edition or len(column.inputs) != len(spec.columns):
            return None
        carried = out.get(spec.edition)
        if carried is None:
            return None
        if not no_edition(carried):
            self.flag("functions", f"output column {column.name} omits the edition part its row carries ({spec.edition})")
            return None
        return Value(carried.bases, carried.labels, flow=carried.flow)

    def project_proofs(self, out: Out, scope: Scope, node: Json) -> None:
        """Rebind a certificate only through exact join equalities in the direction that preserves its selected label."""
        unchanged_domain = (
            len(scope.sources) == 1
            and not scope.sampled
            and not any(node.get(key) for key in ("where_clause", "modifiers", "having", "qualify", "group_expressions", "sample"))
        )
        for index, column in enumerate(out.columns):
            proofs: dict[str, frozenset[Selection]] = {}
            for kind in ("selections", "exclusions"):
                rebound: set[Selection] = set()
                for proof in getattr(column.flow, kind):
                    valid = proof.label == column.flow.occurrences
                    for key, origins, literals in zip(proof.keys, proof.inputs, proof.literals, strict=True):
                        target = out.get(key)
                        reachable = {(origins, literals)}
                        while True:
                            expanded = reachable | {(b.occurrences, b.literals) for a, b in scope.equalities if (a.occurrences, a.literals) in reachable}
                            if expanded == reachable:
                                break
                            reachable = expanded
                        valid &= target is not None and (target.flow.occurrences, target.flow.literals) in reachable
                    if valid:
                        domain = proof.domain if unchanged_domain or kind == "exclusions" else scope.identity
                        rebound.add(self.certificate(proof.scope, out, column, domain))
                proofs[kind] = frozenset(rebound)
            out.columns[index] = replace(column, flow=replace(column.flow, selections=proofs["selections"], exclusions=proofs["exclusions"]))

    def relations(self, ref: Json, scope: Scope) -> None:
        kind = ref["type"]
        if ref.get("sample"):
            scope.sampled = True
        if kind == "BASE_TABLE":
            name = ref["table_name"]
            if not ref["schema_name"] and not ref["catalog_name"] and name in scope.ctes:
                scope.sources.append(self.source(ref.get("alias") or name, self.renamed_output(scope.ctes[name], ref.get("column_name_alias") or [])))
                return
            self.reads.add(name)
            if ref["catalog_name"] not in ("", "staging") or ref["schema_name"] not in ("", "main"):
                self.flag("lineage", f"reads {ref['catalog_name']}.{ref['schema_name']}.{name}, outside the build's main schema")
            scope.sources.append(self.source(ref.get("alias") or name, None, name))
            if ref.get("column_name_alias"):
                self.flag("functions", "physical column alias list was not analyzed (unsupported shape): source column order unavailable")
        elif kind == "JOIN":
            self.relations(ref["left"], scope)
            left = list(scope.sources)
            self.relations(ref["right"], scope)
            if ref["join_type"] == "INNER" and len(left) == 1 and len(scope.sources[len(left) :]) == 1:
                for name in ref.get("using_columns", []):
                    using_left = self.column({"class": "COLUMN_REF", "column_names": [left[0].alias, name]}, scope)
                    using_right = self.column({"class": "COLUMN_REF", "column_names": [scope.sources[len(left)].alias, name]}, scope)
                    scope.value_equalities.extend(((using_left, using_right), (using_right, using_left)))
            for name in ref.get("using_columns", []):
                # Qualified references retain their own side. An unqualified USING key follows the preserved side.
                side = scope.sources[len(left) :] if ref["join_type"] == "RIGHT" else left
                if ref["join_type"] == "OUTER":
                    self.flag("functions", "full JOIN USING column was not analyzed (unsupported shape)")
                if side:
                    scope.using[name] = side[0].alias
            if ref.get("condition") and not self.key_join(ref["condition"], scope):
                self.join_condition(ref, left, scope)
        elif kind == "SUBQUERY":
            out = self.query(ref["subquery"]["node"], scope.ctes, Scope(list(scope.sources), scope.ctes, scope.outer))
            names = ref.get("column_name_alias") or []
            scope.sources.append(self.source(ref.get("alias") or "", self.renamed_output(out, names)))
        elif kind == "TABLE_FUNCTION":
            name = str(ref["function"]["function_name"]).lower()
            self.flag("lineage", f"reads through table function {name}")
            self.expr(ref["function"], scope, "predicate")
            scope.sources.append(self.source(ref.get("alias") or name, None, "?"))
        elif kind != "EMPTY":
            self.flag("lineage", f"relation kind {kind} is not supported")

    def join_condition(self, ref: Json, left: list[Source], scope: Scope) -> None:
        """A join's ON condition in the ``join`` clause, and the column equalities it adds in the direction its join type preserves."""
        outer_clause, self.clause = self.clause, "join"
        self.year_slots |= {id(operand) for part in conjunctions(ref["condition"]) for operand in disjuncts(part) if part is not operand}
        self.expr(ref["condition"], scope, "predicate")
        self.clause = outer_clause
        join_type = ref["join_type"]
        for part in conjunctions(ref["condition"]):
            if part.get("type") != "COMPARE_EQUAL" or not is_column(part.get("left")) or not is_column(part.get("right")):
                continue
            a_value, b_value = self.column(part["left"], scope), self.column(part["right"], scope)
            if join_type == "INNER":
                scope.value_equalities.extend(((a_value, b_value), (b_value, a_value)))
            a, b = a_value.flow, b_value.flow
            left_ids = {source.occurrence for source in left}
            right_ids = {source.occurrence for source in scope.sources[len(left) :]}
            if all(origin.path[0] in right_ids for origin in a.occurrences) and all(origin.path[0] in left_ids for origin in b.occurrences):
                a, b = b, a
            if join_type in {"INNER", "LEFT"}:
                scope.equalities.append((b, a))
            if join_type in {"INNER", "RIGHT"}:
                scope.equalities.append((a, b))
            if join_type in {"LEFT", "RIGHT"}:
                scope.left_joins.append((b, a) if join_type == "LEFT" else (a, b))

    # WHERE clause: natural-key value filters, then the ordinary predicate rules.
    def where(self, node: Json, scope: Scope) -> None:
        outer_clause, self.clause = self.clause, "where"
        self.where_parts(node, scope)
        self.clause = outer_clause

    def where_parts(self, node: Json, scope: Scope) -> None:
        parts = conjunctions(node)
        self.where_conjuncts |= {id(part) for part in parts}
        self.mark_conjunctions(node)
        for part in parts:
            if part["class"] == "OPERATOR" and part["type"] == "OPERATOR_IS_NULL" and is_column(part["children"][0]) and scope.projected:
                value = self.column(part["children"][0], scope)
                scope.projected.columns[:] = [
                    replace(column, labels=column.labels - {"label_pending"}, flow=replace(column.flow, null=True))
                    if value.flow.occurrences and same_value(column, value)
                    else column
                    for column in scope.projected.columns
                ]
            if not self.value_space(part, scope):
                self.expr(part, scope, "predicate")
        for part in parts:
            self.label_exclusion(part, scope)

    def label_exclusion(self, node: Json, scope: Scope) -> None:
        """Certify NOT EXISTS only when it excludes every selected row on the exact projected key."""
        if self.model not in self.policy.scopes["label_choice"] or scope.projected is None:
            return
        if node.get("type") != "OPERATOR_NOT" or len(node.get("children", [])) != 1:
            return
        subquery = node["children"][0]
        if subquery.get("class") != "SUBQUERY" or subquery.get("subquery_type") != "EXISTS":
            return
        query = subquery["subquery"]["node"]
        if query["type"] != "SELECT_NODE" or any(query.get(key) for key in ("modifiers", "group_expressions", "having", "qualify", "sample")):
            return
        ref = query["from_table"]
        if (
            ref["type"] != "BASE_TABLE"
            or ref.get("schema_name")
            or ref.get("catalog_name")
            or ref.get("sample")
            or query.get("cte_map", {}).get("map")
            or not query.get("where_clause")
        ):
            return
        selected = scope.ctes.get(ref["table_name"])
        if selected is None:
            return
        selected = self.renamed_output(selected, ref.get("column_name_alias") or [])
        alias = ref.get("alias") or ref["table_name"]
        keys = self.policy.scopes["label_choice"][self.model]
        parts = conjunctions(query["where_clause"])
        matched: set[str] = set()
        for part in parts:
            if part.get("type") != "COMPARE_EQUAL" or not is_column(part.get("left")) or not is_column(part.get("right")):
                return
            left, right = part["left"], part["right"]
            if len(right["column_names"]) == 2 and right["column_names"][0] == alias:
                left, right = right, left
            names = left["column_names"]
            if len(names) != 2 or names[0] != alias or names[1] not in keys:
                return
            # An unqualified or shadowed name belongs to the inner query, not the correlated outer key.
            if len(right["column_names"]) != 2 or right["column_names"][0] == alias:
                return
            projected = scope.projected.get(names[1])
            if projected is None or not same_value(projected, self.column(right, scope)):
                return
            matched.add(names[1])
        if matched != set(keys) or len(parts) != len(keys):
            return
        label_name = "measure_name" if self.model == "dim_measure" else "code_label"
        selected_label = selected.get(label_name)
        if selected_label is None:
            return
        certificates = [proof for proof in selected_label.flow.selections if proof_matches(proof, selected, selected_label)]
        scope.projected.columns[:] = [
            replace(
                column,
                flow=replace(column.flow, exclusions=frozenset(self.certificate(proof.scope, scope.projected, column, proof.domain) for proof in certificates)),
            )
            if column.name == label_name and column.flow.null
            else column
            for column in scope.projected.columns
        ]

    def mark_conjunctions(self, node: Json) -> None:
        """The AND nodes that ``where`` took apart are visited through their parts."""
        if node["class"] == "CONJUNCTION" and node["type"] == "CONJUNCTION_AND":
            self.seen.add(id(node))
            for child in node["children"]:
                self.mark_conjunctions(child)

    def value_space(self, node: Json, scope: Scope) -> bool:
        """The model's permitted filter on its natural-key column."""
        rule = self.policy.scopes["value_space"].get(self.model)
        if rule is None:
            return False
        function, column, argument = rule
        call = node
        if function == "length":
            if (
                node["class"] != "COMPARISON"
                or node["type"] != "COMPARE_EQUAL"
                or node["right"]["class"] != "CONSTANT"
                or node["right"]["value"]["value"] != argument
            ):
                return False
            call = node["left"]
        if call["class"] != "FUNCTION" or call["function_name"] != function or not call["children"] or column_name(call["children"][0]) != column:
            return False
        if function == "regexp_matches" and not (len(call["children"]) == 2 and is_text(call["children"][1], str(argument))):
            return False
        if function == "length" and len(call["children"]) != 1:
            return False
        value = self.expr(call["children"][0], scope, "predicate")
        allowed = {tuple(pair) for pair in self.policy.scopes["normalization_inputs"].get(self.model, {}).get(column, [])}
        projected = scope.projected.get(column) if scope.projected else None
        local = {source.occurrence for source in scope.sources}
        if (
            not value.bases
            or not value.bases <= allowed
            or not value.flow.occurrences
            or any(origin.path[0] not in local for origin in value.flow.occurrences)
            or (projected is not None and not same_value(projected, value))
        ):
            self.flag("functions", f"A43 {function} input is not bound to the approved natural-key source/output {column}")
            return False
        self.value_filters.append((column, value))
        self.mark(node)
        return True

    # Select-list items and their output roles.
    def item(self, node: Json, scope: Scope) -> tuple[list[Col], list[StarSource]]:
        if node["class"] == "STAR":
            return self.star(node, scope)
        alias = node.get("alias") or column_name(node) or "expression"
        self.projected_calls |= item_calls(node)
        value = self.output(node, alias, scope)
        return [Col(alias, value.bases, value.labels, value.inputs, value.flow)], []

    def star(self, node: Json, scope: Scope) -> tuple[list[Col], list[StarSource]]:
        self.seen.add(id(node))
        if node.get("qualified_exclude_list"):
            self.flag("functions", "qualified STAR exclusions were not analyzed (unsupported shape)")
        replaced = {entry["key"]: self.expr(entry["value"], scope, "output") for entry in node.get("replace_list", [])}
        if node.get("expr"):
            self.expr(node["expr"], scope, "output")
        excluded = set(node.get("exclude_list", []))
        columns: list[Col] = []
        relations: list[StarSource] = []
        for source in scope.sources:
            if node["relation_name"] and source.alias != node["relation_name"]:
                continue
            if source.out is None:
                if node.get("rename_list") or node.get("replace_list") or excluded:
                    self.flag("functions", "physical STAR modifiers were not analyzed (unsupported shape): source columns unavailable")
                relations.append(StarSource(source.relation, (source.occurrence,)))
                continue
            relations += list(source.out.star_relations)
            for column in source.out.columns:
                if column.name in excluded:
                    continue
                renamed = next(
                    (
                        entry["value"]
                        for entry in node.get("rename_list", [])
                        if entry["key"]["column"] == column.name and entry["key"].get("table", "") in {"", source.alias}
                    ),
                    column.name,
                )
                if column.name in replaced:
                    value = replaced[column.name]
                    columns.append(Col(renamed, value.bases, value.labels, value.inputs, value.flow))
                else:
                    columns.append(replace(column, name=renamed))
        return columns, relations

    def output(self, node: Json, alias: str, scope: Scope) -> Value:
        """An output expression. A computed value in a role the policy names (a key, an identity, a date part) must have that role's shape."""
        if is_column(node) or constant_cast(node):
            return self.expr(node, scope, "output")
        normalized = self.normalization(node, alias, scope)
        if normalized is not None:
            return normalized
        matcher = self.role(alias)
        if matcher is None:
            return self.expr(node, scope, "output")
        value = matcher(node, scope)
        if value is not None:
            inherited = self.operand_labels(node, scope)
            occurrences = self.operand_occurrences(node, scope)
            if "calendar_date" in value.labels:
                inherited -= {"date_bound:min", "date_bound:max"}
            if value.labels & {"md5_key", "identity", "county_membership"}:
                inherited = frozenset(label for label in inherited if not label.startswith(("normalization:", "normalize:")))
            self.mark(node)
            return replace(value, labels=value.labels | inherited, flow=replace(value.flow, occurrences=occurrences))
        self.flag("functions", f"output column {alias} is not the contracted expression for that column")
        return self.expr(node, scope, "output")

    def operand_labels(self, node: Json, scope: Scope) -> frozenset[str]:
        """Retain use restrictions when a recognized output matcher consumes its operands."""
        if node["class"] == "CONSTANT":
            return frozenset()
        if is_column(node):
            labels = self.column(node, scope).labels - COLUMN - CONSTANT - OTHER - {"calendar_date", "date_part"}
            return labels - EDITION_BOUND if id(node) in self.edition_parts else labels
        return frozenset(label for child in child_nodes(node) for label in self.operand_labels(child, scope))

    def operand_occurrences(self, node: Json, scope: Scope) -> frozenset[Occurrence]:
        if node["class"] == "CONSTANT":
            return frozenset()
        if is_column(node):
            return self.column(node, scope).flow.occurrences
        return frozenset(origin for child in child_nodes(node) for origin in self.operand_occurrences(child, scope))

    def role(self, alias: str) -> Matcher | None:
        """The matcher for a computed output with this name in this model, or None."""
        if self.kind == "fact":
            mapping = self.fact_map.get(alias)
            return (lambda node, scope: self.unknown_coalesce(node, scope, mapping)) if mapping else None
        if self.kind == "bridge":
            bridge_spec = BRIDGE_KEYS.get(self.model, {}).get(alias)
            return (lambda node, scope: self.key_hash(node, scope, bridge_spec)) if bridge_spec is not None else None
        spec = DIMENSION_KEYS.get(self.model)
        if spec is not None and alias == dimension_key(self.model):
            return lambda node, scope: self.key_hash(node, scope, spec)
        identity = IDENTITY_OUTPUTS.get(self.model)
        if identity and alias == identity[0]:
            return lambda node, scope: self.identity(node, scope, identity[1])
        if (self.model, alias) == (STATE_PREFIX.model, STATE_PREFIX.output):
            return self.state_prefix
        if (self.model, alias) == (COUNTY_MEMBERSHIP.model, COUNTY_MEMBERSHIP.output):
            return self.county_membership
        if self.model == "dim_date":
            if alias == CALENDAR_COLUMN:
                return self.calendar_date
            if alias == FISCAL_COLUMN:
                return self.fiscal_year
            if alias in self.policy.scopes["date_roles"]:
                return lambda node, scope: self.date_part(node, scope, frozenset(self.policy.scopes["date_roles"][alias]))
        return None

    # Roles.
    def key_hash(self, node: Json, scope: Scope, spec: KeySpec) -> Value | None:
        """``md5`` of the natural key's text, or ``md5('<unknown>')``. Each ordered operand keeps physical lineage and occurrence/value identity."""
        if node["class"] != "FUNCTION" or node["function_name"] != "md5" or len(node["children"]) != 1 or "md5_key" not in self.policy.capabilities:
            return None
        argument = node["children"][0]
        if is_text(argument, UNKNOWN):
            return Value(EMPTY, frozenset({"md5_key"}), flow=Flow(literals=frozenset({UNKNOWN})))
        parts = self.key_text(argument, scope, spec)
        if parts is None:
            return None
        return Value(frozenset(base for part in parts for base in part.bases), frozenset({"md5_key"}), parts)

    def key_text(self, node: Json, scope: Scope, spec: KeySpec) -> tuple[Value, ...] | None:
        """Resolved value of each natural-key column in one of the contracted forms, or None."""
        caps = self.policy.capabilities
        if len(spec.columns) > 1:
            return self.concat(node, scope, spec)
        name = spec.columns[0]
        if is_column(node):
            if column_name(node) != name:
                return None
            value = self.column(node, scope)
            self.use_normalizations(value, name)
            return (value,)
        if node["class"] == "CAST" and not node["try_cast"] and node["cast_type"]["id"] == "VARCHAR" and column_name(node["child"]) == name:
            return (self.column(node["child"], scope),) if "varchar_cast" in caps else None
        if node["class"] == "CASE" and "key_case" in caps:
            return self.guard(node, scope, name)
        return None

    def concat(self, node: Json, scope: Scope, spec: KeySpec) -> tuple[Value, ...] | None:
        """``concat_ws(separator, c1, c2, ...)`` over the spec's columns, each plain or cast to varchar."""
        parts = node["children"] if node["class"] == "FUNCTION" and node["function_name"] == "concat_ws" else []
        names = key_columns(spec, len(parts) - 1)
        if len(parts) != len(names) + 1 or not is_text(parts[0], spec.separator):
            return None
        values: list[Value] = []
        for part, name in zip(parts[1:], names, strict=True):
            if is_column(part) and column_name(part) == name:
                value = self.column(part, scope)
                if name == spec.edition and not self.edition_part(value, spec):
                    return None
                self.use_normalizations(value, name)
                values.append(value)
                self.take_edition(part, name, spec)
            elif (
                part["class"] == "CAST"
                and not part["try_cast"]
                and part["cast_type"]["id"] == "VARCHAR"
                and column_name(part["child"]) == name
                and "varchar_cast" in self.policy.capabilities
            ):
                value = self.column(part["child"], scope)
                if name == spec.edition and not self.edition_part(value, spec):
                    return None
                values.append(value)
                self.take_edition(part["child"], name, spec)
            else:
                return None
        return tuple(values)

    def edition_part(self, value: Value, spec: KeySpec) -> bool:
        """What a key may hash as its edition: a bounded ``min`` of a date, a typed null or the stored column the key's spec names (in its pinned model)."""
        stored = spec.edition_from is not None and value.bases == {spec.edition_from} and self.granted("edition_year")
        return bounded_edition(value) or no_edition(value) or stored

    def take_edition(self, column: Json, name: str, spec: KeySpec) -> None:
        """The edition part of a key consumes the bound label (``min``); no other part of a key may carry it."""
        if spec.edition and name == spec.edition:
            self.edition_parts.add(id(column))

    def guard(self, node: Json, scope: Scope, name: str) -> tuple[Value, ...] | None:
        """``case when c is null then '<unknown>' else cast(c as varchar) end`` on the key column; the cast needs its own allowlist entry."""
        checks = node["case_checks"]
        if len(checks) != 1 or not node.get("else_expr"):
            return None
        when, then, other = checks[0]["when_expr"], checks[0]["then_expr"], node["else_expr"]
        if not (when["class"] == "OPERATOR" and when["type"] == "OPERATOR_IS_NULL" and column_name(when["children"][0]) == name and is_text(then, UNKNOWN)):
            return None
        checked = self.column(when["children"][0], scope)
        operand = other["child"] if other["class"] == "CAST" else other
        if not is_column(operand) or not same_value(checked, self.column(operand, scope)):
            return None
        if is_column(other) and column_name(other) == name:
            return (self.column(other, scope),)
        if other["class"] == "CAST" and not other["try_cast"] and other["cast_type"]["id"] == "VARCHAR" and column_name(other["child"]) == name:
            return (self.column(other["child"], scope),) if "varchar_cast" in self.policy.capabilities else None
        return None

    def normalization(self, node: Json, alias: str, scope: Scope) -> Value | None:
        """Approve a text cast by resolved source input and retain its allowed final use roles through every projection."""
        if self.kind != "dimension" or node["class"] != "CAST" or node["try_cast"] or node["cast_type"]["id"] != "VARCHAR" or not is_column(node["child"]):
            return None
        value = self.column(node["child"], scope)
        rules = self.policy.scopes["normalization_inputs"].get(self.model, {})
        roles = [role for role, pairs in rules.items() if value.bases and value.bases <= {tuple(pair) for pair in pairs}]
        if not roles or value.labels - COLUMN - {"label_pending"}:
            return None
        if "varchar_cast" not in self.policy.capabilities:
            self.flag("functions", f"cast of {alias} to varchar needs the 'cast(<natural key> as varchar)' allowlist entry")
            return None
        self.mark(node)
        token = f"normalization:{len(self.normalizations)}"
        self.normalizations[token] = set(roles)
        return replace(value, labels=frozenset({token, *("normalize:" + role for role in roles)}) | (value.labels & {"label_pending"}))

    def use_normalizations(self, value: Value | Col, role: str) -> None:
        """A carried operation is consumed only by its approved final role or key input."""
        for token in value.labels & self.normalizations.keys():
            if role in self.normalizations[token]:
                self.used_normalizations.add(token)
            else:
                self.flag("functions", f"normalized source reaches forbidden role {role}")

    def key_join(self, node: Json, scope: Scope) -> bool:
        """A fact's key join: ``md5(<natural key text>) = <dimension alias>.<dimension>_sk`` for a (natural key, dimension) the contract maps."""
        if self.kind != "fact" or node["class"] != "COMPARISON" or node["type"] != "COMPARE_EQUAL" or not scope.sources:
            return False
        hashed, other = node["left"], node["right"]
        if is_column(hashed):
            hashed, other = other, hashed
        if hashed["class"] != "FUNCTION" or hashed["function_name"] != "md5" or len(hashed["children"]) != 1 or not is_column(other):
            return False
        right = scope.sources[-1]
        if right.out is not None or "md5_key" not in self.policy.capabilities or len(other["column_names"]) < 2 or other["column_names"][-2] != right.alias:
            return False
        for dimension, natural in sorted(set(self.fact_map.values())):
            if right.relation != dimension or column_name(other) != dimension_key(dimension):
                continue
            parts = self.key_text(hashed["children"][0], scope, KeySpec((natural,)))
            if parts is not None:
                self.joined[right.alias] = (natural, dimension)
                self.mark(node)
                return True
        return False

    def unknown_coalesce(self, node: Json, scope: Scope, mapping: tuple[str, str]) -> Value | None:
        """A fact's key: ``coalesce(<dimension>.<dimension>_sk, md5('<unknown>'))`` over the join that hashes the contract's natural key into that dimension."""
        caps = self.policy.capabilities
        if node["class"] != "OPERATOR" or node["type"] != "OPERATOR_COALESCE" or not {"unknown_coalesce", "md5_key"} <= caps:
            return None
        dimension, natural = mapping
        children = node["children"]
        if len(children) != 2 or not is_column(children[0]) or not is_unknown_hash(children[1]):
            return None
        names = children[0]["column_names"]
        if column_name(children[0]) != dimension_key(dimension) or len(names) < 2 or self.joined.get(names[-2]) != (natural, dimension):
            return None
        return Value(self.column(children[0], scope).bases, frozenset({"unknown_coalesce"}))

    def identity(self, node: Json, scope: Scope, spec: KeySpec) -> Value | None:
        """dim_measure's identity text: ``concat_ws('|', fact, measure_key, value_column)``."""
        parts = self.concat(node, scope, spec)
        return None if parts is None else Value(frozenset(base for part in parts for base in part.bases), frozenset({"identity"}), parts)

    def state_prefix(self, node: Json, scope: Scope) -> Value | None:
        """``left(county_fips, 2)`` over the RUCC county code (allowlist entry, dim_state's ``state_fips`` only)."""
        if not self.granted("state_prefix") or node["class"] != "FUNCTION" or node["function_name"] != "left" or len(node["children"]) != 2:
            return None
        code, width = node["children"]
        if (
            column_name(code) != STATE_PREFIX.column
            or width["class"] != "CONSTANT"
            or width["value"]["is_null"]
            or width["value"]["value"] != STATE_PREFIX.width
        ):
            return None
        value = self.column(code, scope)
        return Value(value.bases, frozenset({"state_prefix"})) if value.bases == {(STATE_PREFIX.relation, STATE_PREFIX.column)} else None

    def county_membership(self, node: Json, scope: Scope) -> Value | None:
        """``<county_fips> is not null`` over the county code of the latest list (allowlist entry, ``in_latest_county_list`` only)."""
        if not self.granted("county_list_bounds") or node["class"] != "OPERATOR" or node["type"] != "OPERATOR_IS_NOT_NULL":
            return None
        if len(node["children"]) != 1 or column_name(node["children"][0]) != COUNTY_MEMBERSHIP.column:
            return None
        code = self.column(node["children"][0], scope)
        if LATEST_LIST not in code.labels:
            return None
        self.use_normalizations(code, COUNTY_MEMBERSHIP.column)
        self.membership_selects.add(scope.identity)
        return Value(code.bases, frozenset({"county_membership"}))

    def county_list_proofs(self, node: Json, scope: Scope, out: Out) -> None:
        """The three parts of the membership test in dim_county: the newest vintage of the list, the list's rows of that vintage and the left join."""
        if self.model != COUNTY_MEMBERSHIP.model or not self.granted("county_list_bounds"):
            return
        self.latest_vintage(node, scope, out)
        self.latest_members(node, scope, out)
        self.membership_join(scope, out)

    def list_filter(self, node: Json, scope: Scope) -> bool:
        """The WHERE clause is ``list_name = '<the latest list>'``, over the literal column that names each list, and nothing else."""
        parts = conjunctions(node["where_clause"]) if node.get("where_clause") else []
        if len(parts) != 1 or parts[0]["class"] != "COMPARISON" or parts[0]["type"] != "COMPARE_EQUAL":
            return False
        column, literal = parts[0]["left"], parts[0]["right"]
        if not is_column(column) or column_name(column) != LIST_NAME_COLUMN or not is_text(literal, COUNTY_MEMBERSHIP.list_name):
            return False
        value = self.column(column, scope)
        return not value.bases and COUNTY_MEMBERSHIP.list_name in value.flow.literals

    def latest_vintage(self, node: Json, scope: Scope, out: Out) -> None:
        """``select max(vintage) as vintage from <lists> where list_name = '<the latest list>'``: the newest vintage of that list."""
        column = out.get("vintage")
        if len(out.columns) != 1 or column is None or "range_bound:max" not in column.labels or len(scope.sources) != 1:
            return
        if not simple_select(node) or not self.list_filter(node, scope):
            return
        out.columns = [replace(column, labels=column.labels | {LATEST_VINTAGE})]

    def latest_members(self, node: Json, scope: Scope, out: Out) -> None:
        """``select <lists>.county_fips from <lists> inner join <latest vintage> on <lists>.vintage = <latest vintage>.vintage where list_name = '<list>'``."""
        column, ref = out.get(COUNTY_MEMBERSHIP.column), node["from_table"]
        if len(out.columns) != 1 or column is None or ref["type"] != "JOIN" or ref["join_type"] != "INNER" or len(scope.sources) != 2:
            return
        if not simple_select(node) or not self.list_filter(node, scope):
            return
        condition = ref.get("condition")
        if condition is None or condition["class"] != "COMPARISON" or condition["type"] != "COMPARE_EQUAL":
            return
        if not is_column(condition["left"]) or not is_column(condition["right"]):
            return
        rows, newest = scope.sources
        sides = [self.column(condition["left"], scope), self.column(condition["right"], scope)]
        listed = [value for value in sides if from_source(value, rows) and LATEST_VINTAGE not in value.labels]
        latest = [value for value in sides if from_source(value, newest) and LATEST_VINTAGE in value.labels]
        if len(listed) != 1 or len(latest) != 1 or not listed[0].bases <= RANGE_BOUNDS[self.model].inputs or not from_source(column, rows):
            return
        out.columns = [replace(column, labels=column.labels | {LATEST_LIST})]

    def membership_join(self, scope: Scope, out: Out) -> None:
        """The membership test is the optional side of a left join whose condition equates it with the model's county code."""
        column, key = out.get(COUNTY_MEMBERSHIP.output), out.get(COUNTY_MEMBERSHIP.column)
        if column is None or scope.identity not in self.membership_selects:
            return
        if key is None or not any(
            optional.occurrences == column.flow.occurrences and preserved.occurrences == key.flow.occurrences for optional, preserved in scope.left_joins
        ):
            self.flag("functions", f"output column {column.name} is not a left join of the latest list on {COUNTY_MEMBERSHIP.column}")

    def calendar_date(self, node: Json, scope: Scope) -> Value | None:
        """``cast(unnest(generate_series(<first day>, <last day>, interval 1 day)) as date)`` between source dates."""
        if node["class"] != "CAST" or node["try_cast"] or node["cast_type"]["id"] != "DATE":
            return None
        unnest = node["child"]
        if unnest["class"] != "FUNCTION" or unnest["function_name"] != "unnest" or len(unnest["children"]) != 1:
            return None
        series = unnest["children"][0]
        if series["class"] != "FUNCTION" or series["function_name"] != "generate_series" or len(series["children"]) != 3:
            return None
        first, last, step = series["children"]
        if not (is_column(first) and is_column(last) and interval(step, "to_days", 1)):
            return None
        bases = self.column(first, scope).bases | self.column(last, scope).bases
        if not bases or not bases <= self.policy.date_pairs:
            return None
        return Value(bases, frozenset({"calendar_date"}))

    def date_of(self, node: Json | None, scope: Scope) -> Value | None:
        """The calendar date column a date part reads."""
        if node is None or column_name(node) != CALENDAR_COLUMN:
            return None
        return self.column(node, scope)

    def date_part(self, node: Json, scope: Scope, functions: frozenset[str]) -> Value | None:
        if node["class"] != "FUNCTION" or str(node["function_name"]).lower() not in functions or len(node["children"]) != 1:
            return None
        value = self.date_of(node["children"][0], scope)
        return None if value is None else Value(value.bases, frozenset({"date_part"}))

    def part_of(self, node: Json | None, name: str, scope: Scope) -> Value | None:
        """``month(d)`` or ``year(d)`` of the calendar date, also written ``date_part('month', d)``."""
        if node is None or node["class"] != "FUNCTION":
            return None
        function, kids = str(node["function_name"]).lower(), node["children"]
        if function == name and len(kids) == 1:
            return self.date_of(kids[0], scope)
        if function == "date_part" and len(kids) == 2 and is_text(kids[0], name):
            return self.date_of(kids[1], scope)
        return None

    def fiscal_year(self, node: Json, scope: Scope) -> Value | None:
        """The federal fiscal year (October 1 starts the next one): ``year(date_add(d, interval 3 month))`` or the one-``case`` form."""
        if node["class"] == "FUNCTION" and str(node["function_name"]).lower() == "year" and len(node["children"]) == 1:
            shifted = node["children"][0]
            if (
                shifted["class"] == "FUNCTION"
                and shifted["function_name"] == "date_add"
                and len(shifted["children"]) == 2
                and interval(shifted["children"][1], "to_months", 3)
            ):
                value = self.date_of(shifted["children"][0], scope)
                return None if value is None else Value(value.bases, frozenset({"fiscal_year"}))
            return None
        if node["class"] != "CASE":
            return None
        checks = node["case_checks"]
        if len(checks) != 1 or not node.get("else_expr"):
            return None
        when, then = checks[0]["when_expr"], checks[0]["then_expr"]
        month = self.part_of(when.get("left"), "month", scope) if when["class"] == "COMPARISON" and when["type"] == "COMPARE_GREATERTHANOREQUALTO" else None
        threshold = when.get("right") or {}
        plus = then["class"] == "FUNCTION" and then["function_name"] == "+" and len(then["children"]) == 2
        year_then = self.part_of(then["children"][0], "year", scope) if plus else None
        one = plus and then["children"][1]["class"] == "CONSTANT" and then["children"][1]["value"]["value"] == 1
        year_else = self.part_of(node["else_expr"], "year", scope)
        if month and year_then and year_else and one and threshold.get("class") == "CONSTANT" and threshold["value"]["value"] == 10:
            return Value(month.bases, frozenset({"fiscal_year"}))
        return None

    # Expressions.
    def constant_flow(self, node: Json, scope: Scope) -> Flow:
        """Keep row provenance for literals too: a UNION of constants can vary between occurrences."""
        paths: set[tuple[int, ...]] = set()
        for source in scope.sources:
            source_paths = (
                {origin.path for column in source.out.columns for origin in column.flow.occurrences} | {star.path for star in source.out.star_relations}
                if source.out is not None
                else set()
            )
            paths |= source_paths or {(source.occurrence,)}
        return Flow(
            frozenset(Occurrence(path, "$literal:" + repr(node["value"].get("value"))) for path in paths),
            null=node["value"]["is_null"],
            literals=frozenset({node["value"]["value"]}) if is_text(node) else frozenset(),
        )

    def expr(self, node: Json, scope: Scope, position: str) -> Value:
        self.seen.add(id(node))
        kind = node["class"]
        if kind == "COLUMN_REF":
            return self.column(node, scope)
        if kind == "CONSTANT":
            return Value(EMPTY, CONSTANT, flow=self.constant_flow(node, scope))
        if kind == "FUNCTION":
            return self.function(node, scope, position)
        if kind == "CASE":
            self.flag("case", "case outside the contracted key and fiscal-year roles")
            return self.children(node, scope)
        if kind == "CAST":
            return self.cast(node, scope, position)
        if kind == "WINDOW":
            self.flag("functions", f"window expression {node.get('function_name')} is not allowed (de-duplicate with distinct, group by or any_value)")
            return self.children(node, scope)
        if kind == "OPERATOR" and node["type"] == "OPERATOR_COALESCE":
            self.flag("functions", "coalesce is allowed only as a fact key: coalesce(<dimension>.<dimension>_sk, md5('<unknown>'))")
            return self.children(node, scope)
        if kind == "COMPARISON":
            self.edition_year(node, scope)
        if kind in ("OPERATOR", "COMPARISON", "CONJUNCTION", "BETWEEN") and (
            node["type"] in BOOLEAN_TYPES or str(node["type"]).startswith(("COMPARE_", "CONJUNCTION_"))
        ):
            if position == "output":
                self.flag("functions", f"boolean expression {node['type']} in an output column")
            return self.children(node, scope)
        if kind == "SUBQUERY":
            out = self.query(node["subquery"]["node"], scope.ctes, scope)
            values = [Value(column.bases, column.labels, column.inputs, column.flow) for column in out.columns]
            if node.get("child"):
                values.append(self.expr(node["child"], scope, "predicate"))
            if node.get("subquery_type") == "SCALAR" and len(values) == 1:
                return values[0]
            if position == "output":
                self.flag("functions", "boolean subquery in an output column")
            return Value(
                frozenset(base for value in values for base in value.bases),
                OTHER | frozenset(label for value in values for label in value.labels),
                flow=Flow(frozenset(origin for value in values for origin in value.flow.occurrences)),
            )
        self.flag("functions", f"expression kind {kind} is not supported")
        return self.children(node, scope)

    def children(self, node: Json, scope: Scope) -> Value:
        """Visit every expression directly under a node, whatever field holds it, as a predicate-position operand."""
        bases: set[Base] = set()
        labels: set[str] = set(OTHER)
        occurrences: set[Occurrence] = set()
        for child in child_nodes(node):
            value = self.expr(child, scope, "predicate")
            bases |= value.bases
            labels |= value.labels - COLUMN - CONSTANT - OTHER
            occurrences |= value.flow.occurrences
        return Value(frozenset(bases), frozenset(labels), flow=Flow(frozenset(occurrences)))

    def column(self, node: Json, scope: Scope) -> Value:
        self.seen.add(id(node))
        names = node["column_names"]
        name = names[-1]
        qualifier = names[-2] if len(names) >= 2 else scope.using.get(name)
        candidates = [source for source in scope.sources if qualifier is None or source.alias == qualifier]
        values: list[Value] = []
        for source in candidates:
            if source.out is None:
                values.append(Value(frozenset({(source.relation, name)}), COLUMN, flow=Flow(frozenset({Occurrence((source.occurrence,), name)}))))
                continue
            column = source.out.get(name)
            if column is not None:
                value = Value(column.bases, column.labels, column.inputs, column.flow)
                # ORDER BY and DISTINCT ON resolve output aliases before FROM bindings.
                if source.alias == "" and source.occurrence == 0:
                    return value
                values.append(value)
            elif source.out.star_relations:
                values += [
                    Value(frozenset({(star.relation, name)}), COLUMN, flow=Flow(frozenset({Occurrence(star.path, name)}))) for star in source.out.star_relations
                ]
        if not values:
            if scope.outer is not None and (qualifier is None or not candidates):
                return self.column(node, scope.outer)
            self.flag("functions", f"column {'.'.join(names)} was not analyzed (unsupported shape): unresolved binding")
            return Value(frozenset({("?", name)}), COLUMN)
        if len(values) != 1:
            self.flag("functions", f"column {'.'.join(names)} was not analyzed (unsupported shape): ambiguous occurrence")
            return Value(
                frozenset(base for value in values for base in value.bases),
                frozenset(label for value in values for label in value.labels),
                flow=Flow(frozenset(origin for value in values for origin in value.flow.occurrences)),
            )
        value = values[0]
        label_pairs = {tuple(pair) for pair in self.policy.scopes["normalization_inputs"].get(self.model, {}).get("code_label", [])}
        if self.model == "dim_measure":
            label_pairs = {
                (relation, "measure_name") for relation, _column in self.policy.scopes["normalization_inputs"].get(self.model, {}).get("measure_key", [])
            }
        if self.model in self.policy.scopes["label_choice"] and value.bases & label_pairs and not value.flow.null:
            value = replace(value, labels=value.labels | {"label_pending"})
        return value

    def text_of_bound(self, child: Json, value: Value) -> bool:
        """The text form of a label group's ``min`` or ``max``, as dim_measure writes its edition bounds."""
        bound = RANGE_BOUNDS.get(self.model)
        return (
            bound is not None
            and bound.capability == "label_bounds"
            and self.granted(bound.capability)
            and child["class"] == "FUNCTION"
            and str(child["function_name"]).lower() in {"min", "max"}
            and bool(value.labels & {"range_bound:min", "range_bound:max"})
        )

    def cast(self, node: Json, scope: Scope, position: str) -> Value:
        target, child = node["cast_type"]["id"], node["child"]
        value = self.expr(child, scope, position)
        if node["try_cast"]:
            allowed = (
                self.model == self.policy.scopes["release"]["model"]
                and target == "DATE"
                and value.bases == {(self.policy.scopes["release"]["relation"], self.policy.scopes["release"]["publication_column"])}
            )
            if not allowed:
                self.flag("functions", f"try_cast to {target} is outside the contracted release-date source scope")
            return replace(value, labels=value.labels - COLUMN - CONSTANT | {"release_date"})
        if constant_cast(child):
            return value
        if target == "VARCHAR" and self.text_of_bound(child, value):
            return value
        self.flag("functions", f"cast to {target} of a column is allowed only as the varchar normalization of a dimension's natural-key or code column")
        return replace(value, labels=value.labels | OTHER)

    def edition_year(self, node: Json, scope: Scope) -> None:
        """Approve ``left(<edition bound>, 4)`` in the comparison of the control bridge's edition join with the seed vintage range of its seed row."""
        model, relation, width, pairs, seeds = EDITION_YEAR
        call, seed = node["left"], node["right"]
        if self.model != model or not self.granted("edition_year") or self.clause != "join" or not is_column(seed):
            return
        if id(node) not in self.year_slots or call["class"] != "FUNCTION" or call["function_name"] != "left" or len(call["children"]) != 2:
            return
        bound, size = call["children"]
        pair = pairs.get(column_name(bound) or "")
        if pair is None or node["type"] != pair[0] or column_name(seed) != pair[1] or size["class"] != "CONSTANT" or size["value"]["is_null"]:
            return
        if size["value"]["value"] != width or len(scope.sources) != 2:
            return
        rows, editions = scope.sources
        edition, vintage = self.column(bound, scope), self.column(seed, scope)
        from_editions = edition.bases == {(relation, column_name(bound) or "")} and from_source(edition, editions)
        from_seed = from_source(vintage, rows) and bool(vintage.bases) and vintage.bases <= {(name, pair[1]) for name in seeds}
        if from_editions and from_seed:
            self.year_calls.add(id(call))

    def county_filter(self, node: Json, scope: Scope, position: str) -> bool:
        """``county_fips like '09%'`` over the RUCA rows, a positive conjunct of the WHERE clause of the planning-region bridge's own select."""
        model, relation, column, pattern = COUNTY_FILTER
        if self.model != model or not self.granted("county_prefix") or position != "predicate" or self.clause != "where":
            return False
        if scope.outer is not None or id(node) not in self.where_conjuncts:
            return False
        if len(node["children"]) != 2 or column_name(node["children"][0]) != column or not is_text(node["children"][1], pattern):
            return False
        return self.column(node["children"][0], scope).bases == {(relation, column)}

    def function(self, node: Json, scope: Scope, position: str) -> Value:
        name = str(node["function_name"]).lower()
        if (name == "left" and id(node) in self.year_calls) or (name == "~~" and self.county_filter(node, scope, position)):
            return self.children(node, scope)
        if name in ARITHMETIC:
            self.flag("arithmetic", f"arithmetic {name!r}")
            return self.children(node, scope)
        if name == "md5":
            self.flag("functions", "md5 is allowed only for a surrogate key at its contracted output, or in a fact's key join to its dimension")
            return self.children(node, scope)
        if name in self.policy.aggregates or name == "count_star":
            return self.aggregate(name, node, scope)
        self.flag("functions", f"function {name} is not approved here")
        return self.children(node, scope)

    def tract_rows(self, node: Json, scope: Scope) -> bool:
        """A plain ``count(*)`` select item over the RUCA tract rows of one physical relation, grouped by exactly the old county, planning region and file."""
        return (
            self.model == TRACT_COUNT.model
            and self.granted("tract_count")
            and plain_call(node)
            and id(node) in self.projected_calls
            and len(scope.sources) == 1
            and scope.sources[0].out is None
            and scope.sources[0].relation == TRACT_COUNT.relation
            and len(scope.groups) == len(TRACT_COUNT.grouping)
            and {column_name(group) for group in scope.groups} == TRACT_COUNT.grouping
        )

    def range_bound(self, name: str, node: Json, value: Value, scope: Scope) -> Value | None:
        """A plain ``min`` or ``max`` select item over one column of the relations a model's allowlist entry names; the result may reach only its output column.

        A label group's bound (``label_bounds``) needs a GROUP BY. The column may carry the text cast of its ordering role, which the bound consumes.
        """
        bound = RANGE_BOUNDS.get(self.model)
        if bound is None or not self.granted(bound.capability) or not plain_call(node) or id(node) not in self.projected_calls:
            return None
        if not is_column(node["children"][0]) or not value.bases or not value.bases <= bound.inputs:
            return None
        if bound.capability == "label_bounds" and not scope.groups:
            return None
        rows = {origin.path for origin in value.flow.occurrences}
        if name in bound.grouped_rows and any(not {origin.path for origin in self.column(group, scope).flow.occurrences} <= rows for group in scope.groups):
            return None
        self.use_normalizations(value, "@order")
        labels = frozenset(label for label in value.labels if not label.startswith(("normalization:", "normalize:")))
        return replace(value, labels=labels - OTHER | {"range_bound:" + name})

    def aggregate(self, name: str, node: Json, scope: Scope) -> Value:
        kids = node["children"]
        value = self.children(node, scope)
        if name == "any_value" and len(kids) == 1 and is_column(kids[0]) and plain_call(node):
            return value
        policy = self.policy
        if name == "count_star" and self.model == self.policy.scopes["release"]["model"]:
            return Value(EMPTY, frozenset({"copy_count"}))
        if name == "count_star" and self.tract_rows(node, scope):
            return Value(EMPTY, frozenset({"tract_count"}))
        if name in ("min", "max") and len(kids) == 1 and value.bases:
            if (
                self.model in policy.scopes["source_date_models"]
                and value.bases <= frozenset(tuple(pair) for pair in policy.scopes.get("source_dates", {}).get(self.model, policy.date_pairs))
                and all(child["class"] == "COLUMN_REF" for child in kids)
            ):
                return replace(value, labels=value.labels - OTHER | {"date_bound:" + name})
            if (
                self.model == self.policy.scopes["release"]["model"]
                and name == "min"
                and value.bases == {(self.policy.scopes["release"]["relation"], self.policy.scopes["release"]["object_column"])}
            ):
                return replace(value, labels=value.labels - OTHER | {"object_key"})
            bound = self.range_bound(name, node, value, scope)
            if bound is not None:
                return bound
        shown = ", ".join(f"{relation}.{column}" for relation, column in sorted(value.bases))
        self.flag("functions", f"aggregate {name}({shown}) is outside the A36 scopes (de-duplicate with distinct, group by or any_value)")
        return replace(value, labels=value.labels | OTHER)


def final_roles(model: str) -> dict[str, set[str]]:
    """The output columns each operation label may reach in a model; a label in any other output column is a finding."""
    range_bound = RANGE_BOUNDS.get(model)
    return {
        "release_date": {"release_date"},
        "copy_count": {"copy_count"},
        "object_key": {"object_key"},
        "date_bound:min": {"first_seen"} if model == "dim_hospital" else set(),
        "date_bound:max": {"last_seen"} if model == "dim_hospital" else set(),
        "state_prefix": {STATE_PREFIX.output} if model == STATE_PREFIX.model else set(),
        "county_membership": {COUNTY_MEMBERSHIP.output} if model == COUNTY_MEMBERSHIP.model else set(),
        "tract_count": {TRACT_COUNT.output} if model == TRACT_COUNT.model else set(),
        "range_bound:min": {range_bound.minimum} if range_bound else set(),
        "range_bound:max": {range_bound.maximum} if range_bound else set(),
    }


def _analyze_model(model: str, kind: str, document: Json, policy: Policy, fact_map: dict[str, tuple[str, str]]) -> Analysis:
    """Parse one compiled model: findings, physical reads and the contracted key columns at the output positions.

    ``fact_map`` maps each of a fact's key columns to the (dimension model, natural-key column) the contract gives it.
    """
    statements = document.get("statements", [])
    walker = Walker(model, kind, policy, fact_map, query_fingerprint(statements[0]) if len(statements) == 1 else "")
    if kind == "dimension" and model not in DIMENSION_KEYS:
        walker.flag("functions", "no natural-key specification for this dimension (add it to DIMENSION_KEYS with its contract amendment)")
    if len(statements) != 1:
        walker.flag("functions", "compiled SQL must contain exactly one statement")
        return Analysis(walker.findings, set(), Out([]))
    top = walker.query(statements[0]["node"], {})
    walker.sweep(document["statements"][0]["node"])
    walker.pin_findings()
    wanted: dict[str, str] = {}
    if kind == "dimension":
        wanted[dimension_key(model)] = "md5_key"
    wanted |= dict.fromkeys(BRIDGE_KEYS.get(model, {}), "md5_key")
    wanted |= dict.fromkeys(fact_map, "unknown_coalesce")
    for key, label in wanted.items():
        column = top.get(key)
        if column is None:
            walker.flag("functions", f"no output column {key}")
        elif column.labels != {label}:
            walker.flag("functions", f"output column {key} is not the contracted expression (found {sorted(column.labels)}, wanted {label})")
        elif label == "md5_key":
            bind_key(walker, model, key, column, top)
    for name, value in walker.value_filters:
        natural = top.get(name)
        if (
            natural is None
            or not value.flow.occurrences
            or any(
                not any(origin.column == final.column and final.path[-len(origin.path) :] == origin.path for final in natural.flow.occurrences)
                for origin in value.flow.occurrences
            )
        ):
            walker.flag("functions", f"A43 predicate input does not reach natural-key output {name}")
    roles = final_roles(model)
    for column in top.columns:
        if "label_pending" in column.labels and not any(proof_matches(proof, top, column) for proof in column.flow.selections):
            walker.flag("functions", f"A48 published label reaches {column.name} without the complete label selection")
        walker.use_normalizations(column, column.name)
        if "md5_key" in column.labels and column.name not in wanted:
            walker.flag("functions", f"surrogate-key operation reaches forbidden final role {column.name}")
        normalization = {label.removeprefix("normalize:") for label in column.labels if label.startswith("normalize:")}
        if normalization and column.name not in normalization:
            walker.flag("functions", f"normalized source reaches forbidden final role {column.name}; allowed roles {sorted(normalization)}")
        for label, permitted in roles.items():
            if label in column.labels and column.name not in permitted:
                walker.flag("functions", f"operation {label} reaches forbidden final role {column.name}")
    for token in walker.normalizations.keys() - walker.used_normalizations:
        walker.flag("functions", f"normalization cast has no approved final role or ordering use ({sorted(walker.normalizations[token])})")
    if model == "dim_date":
        stray = [column.name for column in top.columns if "fiscal_year" in column.labels and column.name != FISCAL_COLUMN]
        if stray:
            walker.flag("case", f"the fiscal-year expression is allowed in {FISCAL_COLUMN} only (found in {stray})")
    return Analysis(walker.findings, walker.reads, top)


def bind_key(walker: Walker, model: str, key: str, column: Col, top: Out) -> None:
    """The columns a surrogate key hashes must be the model's own natural-key output columns, by resolved lineage through every CTE and alias."""
    spec = DIMENSION_KEYS.get(model) if model.startswith("dim_") else BRIDGE_KEYS.get(model, {}).get(key)
    if spec is None:
        return
    names = key_columns(spec, len(column.inputs))
    if len(column.inputs) != len(names):
        walker.flag("functions", f"output column {key} hashes no natural key (found {len(column.inputs)} inputs, wanted {len(spec.columns)})")
        return
    for name, hashed in zip(names, column.inputs, strict=True):
        natural = top.get(name)
        if natural is None:
            if model.startswith("dim_"):
                walker.flag("functions", f"no natural-key output column {name}")
        elif not same_value(natural, hashed):
            walker.flag("functions", f"{key} hashes a column whose lineage or occurrence/value identity differs from the natural-key output {name}")


def canonical_query(node: Any) -> Any:
    """The parse of a query without source positions and without the catalog and schema of a table reference, which keeps one bit: bare (a CTE) or qualified."""
    if isinstance(node, list):
        return [canonical_query(item) for item in node]
    if not isinstance(node, dict):
        return node
    table = node.get("type") == "BASE_TABLE" and "table_name" in node
    dropped = {"query_location", "schema_name", "catalog_name"} if table else {"query_location"}
    result = {key: canonical_query(value) for key, value in node.items() if key not in dropped}
    if table:
        result["qualified"] = bool(node["schema_name"] or node["catalog_name"])
    return result


def query_fingerprint(statement: Json) -> str:
    """The SHA-256 of a statement's canonical parse: equal for the same query whatever its layout, comments, quoting or keyword case."""
    return hashlib.sha256(json.dumps(canonical_query(statement), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def normalized_sql(sql: str) -> tuple[str, ...]:
    """Compare SQL tokens while ignoring whitespace, comments and identifier quoting.

    String literals, numeric spelling, operators, aliases and token order remain significant. Quoted identifiers obey DuckDB's
    case-insensitive identifier rule. A single terminal semicolon is optional; additional statements cannot disappear.
    Unsupported or unterminated quoted text raises PolicyError rather than granting equality.
    """
    tokens: list[str] = []
    offset = 0
    while offset < len(sql):
        if sql[offset].isspace():
            offset += 1
            continue
        if sql.startswith("--", offset):
            end = sql.find("\n", offset)
            offset = len(sql) if end < 0 else end + 1
            continue
        if sql.startswith("/*", offset):
            depth = 1
            offset += 2
            while depth and offset < len(sql):
                if sql.startswith("/*", offset):
                    depth += 1
                    offset += 2
                elif sql.startswith("*/", offset):
                    depth -= 1
                    offset += 2
                else:
                    offset += 1
            if depth:
                raise PolicyError("unterminated SQL comment")
            continue
        quote = sql[offset]
        if quote in "'\"":
            start = offset
            offset += 1
            text: list[str] = []
            while offset < len(sql):
                if sql[offset] == quote:
                    if offset + 1 < len(sql) and sql[offset + 1] == quote:
                        text.append(quote)
                        offset += 2
                        continue
                    offset += 1
                    break
                text.append(sql[offset])
                offset += 1
            else:
                raise PolicyError("unterminated SQL quote")
            tokens.append("string:" + sql[start:offset] if quote == "'" else "identifier:" + "".join(text).lower())
            continue
        word = re.match(r"[A-Za-z_][A-Za-z_0-9$]*", sql[offset:])
        if word:
            tokens.append("identifier:" + word[0].lower())
            offset += len(word[0])
            continue
        number = re.match(r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", sql[offset:])
        if number:
            tokens.append("number:" + number[0])
            offset += len(number[0])
            continue
        operator = next((value for value in ("::", "<=", ">=", "<>", "!=", "||", "//", "**", "->>", "->") if sql.startswith(value, offset)), None)
        if operator:
            tokens.append(operator)
            offset += len(operator)
        elif sql[offset] in "(),.;+-*/%=<>[]:":
            tokens.append(sql[offset])
            offset += 1
        else:
            raise PolicyError(f"unsupported SQL token at offset {offset}")
    if tokens and tokens[-1] == ";":
        tokens.pop()
    return tuple(tokens)


def render_fact(macro_text: str, silver_model: str, grain: str, keys: Json, relations: dict[str, str]) -> str:
    """Render the actual gold_fact macro with contract arguments and the build's relation names; missing context fails closed."""
    # The dbt test blocks follow the macros in this file and are not part of the fact renderer.
    macros = macro_text.split("{% test ", 1)[0]
    environment = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))

    def ref(name: str) -> str:
        if name not in relations:
            raise PolicyError(f"gold_fact reference {name} has no build relation binding")
        return relations[name]

    def compiler_error(message: str) -> str:
        raise PolicyError(message)

    module = environment.from_string(macros).make_module({"ref": ref, "exceptions": {"raise_compiler_error": compiler_error}})
    macro = getattr(module, "gold_fact", None)
    if not isinstance(macro, Macro):
        raise PolicyError("gold_fact macro is absent from the contract macro file")
    return str(macro(silver_model, grain, keys))


def analyze_fact(model: str, document: Json, policy: Policy, mapping: dict[str, tuple[str, str]], compiled: str | None, expected: str | None) -> Analysis:
    """Accept a fact's expression shape only when it equals the macro output; retain independent read and completeness analysis."""
    analysis = _analyze_model(model, "fact", document, policy, mapping)
    matches = False
    reason = "gold_fact comparison has no compiled SQL or rendered contract output"
    if compiled is not None and expected is not None:
        try:
            matches = normalized_sql(compiled) == normalized_sql(expected)
            reason = "compiled SQL differs from gold_fact rendered with this fact's contract entry"
        except PolicyError as error:
            reason = str(error)
    if matches:
        analysis.findings = [
            finding
            for finding in analysis.findings
            if finding.check == "lineage" or any(marker in finding.message for marker in ("not supported", "not analyzed", "exactly one statement"))
        ]
    else:
        analysis.findings.append(Finding("functions", f"{model}: {reason}"))
    return analysis


def analyze_model(model: str, kind: str, document: Json, policy: Policy, fact_map: dict[str, tuple[str, str]]) -> Analysis:
    """Analyze dimensions and bridges; a fact without macro-comparison context fails closed."""
    return analyze_fact(model, document, policy, fact_map, None, None) if kind == "fact" else _analyze_model(model, kind, document, policy, fact_map)
