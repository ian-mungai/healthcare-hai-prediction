"""E2E check of the staging copy, label, twin, sheet, POS, CMI, spine, Care Compare and geography models (failure modes 166 to 173, 177 to 188, 267 to 419).

Run from the repository root with Docker running:

    .venv/bin/python -m scripts.lakehouse.run_staging_e2e            # fixture cases only
    .venv/bin/python -m scripts.lakehouse.run_staging_e2e --real     # then the real bronze tables, built twice
    .venv/bin/python -m scripts.lakehouse.run_staging_e2e --real-only  # the real builds only, when no fixture changed

Each fixture case writes a small bronze database, runs ``dbt build`` against it in the analytics image and compares the
models with expectations computed here, independently of the SQL. Each case runs on its own copy of the dbt project,
whose label and twin seeds are written by the real generator (``ipps_file_labels``) from the fixture's names and whose
POS period seed names the fixture's POS files. The failing cases must fail one named dbt test each: a name clash under
one release, copies with different row counts, a stale label hold, an object with two checksums, an unheld label
conflict, an unlabelled copy, a POS file without a period, a POS value that does not cast, a CCN twice in one POS file,
an unreviewed CMI family, an unknown CMI layout, a CMI that disagrees with its relative weights, a CMI out of range, a
Hospital General Information value that does not cast, a cost report in the wrong file year, a cost-report amount that
does not cast, a geography file without its period, a HUD ratio that is not a number, a ZIP code whose residential ratios
do not sum to 0 or 1, a one-way adjacency edge, a changed RUCC header, an unknown RUCA code and a service-area count that
is not a number.
The real stage checks that the generators reproduce the committed seeds, builds the models from the catalog twice and
reconciles them with bronze.

Failure modes: ``plans/staging_dedup_20261003/failure_modes.md``,
``plans/staging_families_20261003/failure_modes.md``,
``plans/sheet_selection_20261005/failure_modes.md``,
``plans/hospital_spine_20261005/failure_modes.md``,
``plans/group_b_20261005/failure_modes_b1.md`` to ``failure_modes_b5c.md`` in the same folder,
``plans/group_c_20261006/failure_modes_c1.md``. The report in ``data/e2e/staging/`` holds
outcomes and counts, never data values or credentials.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import sys
import threading
import time
from collections.abc import Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import catalog, geography_file_periods, ipps_file_labels, memory_budget, mmd_conditions, ownership_release_periods, pos_file_periods
from scripts.process import run_command

REPO_ROOT = catalog.REPO_ROOT
OUT = REPO_ROOT / "data/analytics/dbt"
CASES = OUT / "e2e"
REPORTS = REPO_ROOT / "data/e2e/staging"
# The memory budget each real build ran with, recorded in the report [463].
BUDGETS: list[dict[str, Any]] = []
CONTAINER_OUT = "/workspace/out"
TIMEOUT = 7200
TABLES = (
    "cms_hai_hospital",
    "cms_hai_state",
    "cms_hai_national",
    "cms_hospital_cost_reports",
    "cms_ipps_text_lines",
    "cms_ipps_sheet_rows",
    "cms_ipps_sas",
    "cms_occupational_mix_text_lines",
    "cms_occupational_mix_text_lines_utf16",
    "cms_occupational_mix_sheet_rows",
    "cms_provider_of_services",
    "cms_cc_hospital_general_information",
    "cms_hospital_enrollments",
    "cms_hospital_owners",
    "cms_change_of_ownership",
    "cms_cc_timely_and_effective_care_hospital",
    "cms_cc_maternal_health_hospital",
    "cms_cc_hcahps_hospital",
    "cms_medicare_inpatient_by_provider",
    "cms_medicare_inpatient_by_drg",
    "hhs_capacity_csv",
    "onc_pi_attestations_csv",
    "onc_pi_chpl_linkage_csv",
    "hud_zip_county",
    "hud_zip_county_sheet_rows",
    "county_adjacency",
    "county_adjacency_2010_text_lines",
    "rucc",
    "rucc_sheet_rows",
    "ruca_tracts_2020",
    "ruca_zip_2020",
    "ruca_zip_2010",
    "ruca_sheet_rows",
    "cms_hsa_csv",
    "svi",
    "acs_dp02",
    "acs_dp03",
    "acs_dp04",
    "acs_dp05",
    "acs_s0101",
    "acs_s0601",
    "acs_s1701",
    "acs_s2503",
    "acs_s2701",
    "acs_b16005",
    "acs_b19013",
    "acs_b25070",
    "acs_b25091",
    "acs_b26001",
    "acs_c16001",
    "acs_summary_b16005",
    "acs_summary_b19013",
    "acs_summary_b25070",
    "acs_summary_b25091",
    "acs_summary_b26001",
    "acs_summary_c16001",
    "saipe_text_lines",
    "sahie",
    "bls_laus",
    "places",
    "cms_geographic_variation_csv",
    "wonder_county_mortality",
    "cms_mmd_csv",
    "hrsa_hpsa_detail",
    "hrsa_mua_detail",
    "cms_cc_unplanned_hospital_visits_hospital",
    "cms_cc_complications_and_deaths_hospital",
    "cms_cc_hospital_readmissions_reduction_program_hospital",
    "cms_cc_hac_reduction_program_hospital",
    "cms_cc_hvbp_tps",
)
HELD = {
    "61a3cfb84973b2997ca60b2ebdce129005a9267d452db0ee984d9ca1eefacc88": "BRZ-016",
    "83b9668d22a23b40def50b64725f9674dcaa821d0b2db5e43215e672cad69a71": "BRZ-016",
}
NESTED_DATE = re.compile(r".*(\d{4}-\d{2}-\d{2})\.zip!")
HAI_FILE = "Healthcare_Associated_Infections-Hospital.csv"


@dataclass(frozen=True)
class Stored:
    """One loaded object in a fixture bronze table."""

    table: str
    key: str
    release: str
    member: str
    sha: str
    rows: int
    snapshot: str = "fixture-snapshot"
    second_sha: str | None = None
    # Text lines, or "sheet:cell|cell" rows for a workbook; empty gives generated values.
    content: tuple[str, ...] = ()
    # Bronze also holds this copy's rows although it is not the loaded copy, as before one copy per file [207].
    force_loaded: bool = False
    # Rows of a wide table (POS, Care Compare, ownership) as (column, value) pairs; absent columns are null.
    records: tuple[tuple[tuple[str, str], ...], ...] = ()
    # The labels the publisher printed under each header, as bronze.column_map keeps them (ACS exports) [420].
    labels: tuple[tuple[str, str], ...] = ()
    # The note lines before the header, as bronze.file_preambles keeps them (SAHIE) [439].
    preamble: tuple[str, ...] = ()


def sha(label: str) -> str:
    """Return a readable 64-character fake checksum."""
    return (label * 64)[:64]


# Displayed values in the text file (4 decimals, "$1,234.50") against full precision in the workbook [189]; data rows align
# by position after the title and header rows [190].
TWIN_TEXT = (
    "FY 2021 IPPS Impact File - Final Rule\t\t",
    "Provider Number\tName\tCMI\tPayment\tShare",
    '010001\t"Alpha, Hospital "\t1.2346\t"$1,234.50"\t1.58%',
    '010002\tBeta\t0.5\t"$60,591,269.77"\t0.22%',
)
TWIN_SHEET = (
    "Variable Descriptions:Variable|Meaning",
    "Data:Provider Number|Name|CMI|Payment|Share",
    "Data:10001|Alpha, Hospital|1.23456789|1234.4978|0.0158371255",
    "Data:10002|Beta|0.5|60591269.765|0.0021846031",
)
OCCMIX_TEXT = ("Provider,Wage", "010001,3.5", "010002,4.0")
# The BRZ-016 text held under two names; its workbook twin agrees with it [268].
HELD_TEXT = ("Provider Number\tCMI", "010001\t1.5")
# A final-rule workbook whose correction-notice sheet has the text's row count but other values [267] [269].
FR_CN_SHEET = (
    "FR 2024:Provider Number|Name|CMI|Payment|Share",
    "FR 2024:10001|Alpha, Hospital|1.23456789|1234.4978|0.0158371255",
    "FR 2024:10002|Beta|0.5|60591269.765|0.0021846031",
    "CN 2024:Provider Number|Name|CMI|Payment|Share",
    "CN 2024:10001|Alpha, Hospital|1.3|1300|0.016",
    "CN 2024:10002|Beta|0.6|60000000|0.0022",
)
HAI_2021 = (
    "010001|HAI_1_SIR|01/01/2019|12/31/2019|0.5",
    "010001|HAI_2_SIR|01/01/2019|12/31/2019|0.7",
    "010002|HAI_1_SIR|01/01/2019|12/31/2019|0.9",
)
HAI_2026_MAY = ("010001|HAI_1_SIR|01/01/2019|12/31/2019|0.6", "010001|HAI_1_SIR|01/01/2025|12/31/2025|1.1")
# A letter-suffixed facility ID is kept as text; a date that does not parse is held, never guessed [233].
HAI_2026_AUG = (
    "010001|HAI_1_SIR|01/01/2025|12/31/2025|1.2",
    "010003|HAI_1_SIR|01/01/2025|12/31/2025|0.4",
    "01000F|HAI_1_SIR|01/01/2025|12/31/2025|0.3",
    "010004|HAI_1_SIR|13/45/2025|12/31/2025|0.2",
)
HAI_STATE = ("AL|HAI_1_SIR|01/01/2019|12/31/2019|0.8", "AL|HAI_2_SIR|01/01/2019|12/31/2019|0.9")
HAI_NATIONAL = ("|HAI_1_SIR|01/01/2019|12/31/2019|1.0",)
HAI_TABLES = {"cms_hai_hospital": "facility_id", "cms_hai_state": "state", "cms_hai_national": None}
PROVENANCE_COLUMNS = (
    "_object_key",
    "_source_id",
    "_snapshot_id",
    "_dataset_id",
    "_release_id",
    "_s3_key",
    "_s3_version_id",
    "_member_path",
    "_member_sha256",
    "_row_number",
)
# Every column of the HCRIS public use file, as bronze names them [336].
COST_REPORT_COLUMNS = (
    "rpt_rec_num",
    "provider_ccn",
    "hospital_name",
    "street_address",
    "city",
    "state_code",
    "zip_code",
    "county",
    "medicare_cbsa_number",
    "rural_versus_urban",
    "ccn_facility_type",
    "provider_type",
    "type_of_control",
    "fiscal_year_begin_date",
    "fiscal_year_end_date",
    "fte_employees_on_payroll",
    "number_of_interns_and_residents_fte",
    "total_days_title_v",
    "total_days_title_xviii",
    "total_days_title_xix",
    "total_days_v_xviii_xix_unknown",
    "number_of_beds",
    "total_bed_days_available",
    "total_discharges_title_v",
    "total_discharges_title_xviii",
    "total_discharges_title_xix",
    "total_discharges_v_xviii_xix_unknown",
    "number_of_beds_total_for_all_subproviders",
    "hospital_total_days_title_v_for_adults_peds",
    "hospital_total_days_title_xviii_for_adults_peds",
    "hospital_total_days_title_xix_for_adults_peds",
    "hospital_total_days_v_xviii_xix_unknown_for_adults_peds",
    "hospital_number_of_beds_for_adults_peds",
    "hospital_total_bed_days_available_for_adults_peds",
    "hospital_total_discharges_title_v_for_adults_peds",
    "hospital_total_discharges_title_xviii_for_adults_peds",
    "hospital_total_discharges_title_xix_for_adults_peds",
    "hospital_total_discharges_v_xviii_xix_unknown_for_adults_peds",
    "cost_of_charity_care",
    "total_bad_debt_expense",
    "cost_of_uncompensated_care",
    "total_unreimbursed_and_uncompensated_care",
    "total_salaries_from_worksheet_a",
    "overhead_non_salary_costs",
    "depreciation_cost",
    "total_costs",
    "inpatient_total_charges",
    "outpatient_total_charges",
    "combined_outpatient_inpatient_total_charges",
    "wage_related_costs_core",
    "wage_related_costs_rhc_fqhc",
    "total_salaries_adjusted",
    "contract_labor_direct_patient_care",
    "wage_related_costs_for_part_a_teaching_physicians",
    "wage_related_costs_for_interns_and_residents",
    "cash_on_hand_and_in_banks",
    "temporary_investments",
    "notes_receivable",
    "accounts_receivable",
    "less_allowances_for_uncollectible_notes_and_accounts_receivable",
    "inventory",
    "prepaid_expenses",
    "other_current_assets",
    "total_current_assets",
    "land",
    "land_improvements",
    "buildings",
    "leasehold_improvements",
    "fixed_equipment",
    "major_movable_equipment",
    "minor_equipment_depreciable",
    "health_information_technology_designated_assets",
    "total_fixed_assets",
    "investments",
    "other_assets",
    "total_other_assets",
    "total_assets",
    "accounts_payable",
    "salaries_wages_and_fees_payable",
    "payroll_taxes_payable",
    "notes_and_loans_payable_short_term",
    "deferred_income",
    "other_current_liabilities",
    "total_current_liabilities",
    "mortgage_payable",
    "notes_payable",
    "unsecured_loans",
    "other_long_term_liabilities",
    "total_long_term_liabilities",
    "total_liabilities",
    "general_fund_balance",
    "total_fund_balances",
    "total_liabilities_and_fund_balances",
    "drg_amounts_other_than_outlier_payments",
    "drg_amounts_before_october_1",
    "drg_amounts_after_october_1",
    "outlier_payments_for_discharges",
    "disproportionate_share_adjustment",
    "allowable_dsh_percentage",
    "managed_care_simulated_payments",
    "total_ime_payment",
    "inpatient_revenue",
    "outpatient_revenue",
    "total_patient_revenue",
    "less_contractual_allowance_and_discounts_on_patients_accounts",
    "net_patient_revenue",
    "less_total_operating_expense",
    "net_income_from_service_to_patients",
    "total_other_income",
    "total_income",
    "total_other_expenses",
    "net_income",
    "cost_to_charge_ratio",
    "net_revenue_from_medicaid",
    "medicaid_charges",
    "net_revenue_from_stand_alone_chip",
    "stand_alone_chip_charges",
)
# The bronze columns each wide fixture table carries; the POS list is every column the POS models read.
MUP_PROVIDER_COLUMNS = (
    "rndrng_prvdr_ccn",
    "rndrng_prvdr_org_name",
    "rndrng_prvdr_st",
    "rndrng_prvdr_city",
    "rndrng_prvdr_zip5",
    "rndrng_prvdr_state_abrvtn",
    "rndrng_prvdr_state_fips",
    "rndrng_prvdr_ruca",
    "rndrng_prvdr_ruca_desc",
    "tot_benes",
    "tot_submtd_cvrd_chrg",
    "tot_pymt_amt",
    "tot_mdcr_pymt_amt",
    "tot_dschrgs",
    "tot_cvrd_days",
    "tot_days",
    "bene_avg_age",
    "bene_age_lt_65_cnt",
    "bene_age_65_74_cnt",
    "bene_age_75_84_cnt",
    "bene_age_gt_84_cnt",
    "bene_feml_cnt",
    "bene_male_cnt",
    "bene_race_wht_cnt",
    "bene_race_black_cnt",
    "bene_race_api_cnt",
    "bene_race_hspnc_cnt",
    "bene_race_natind_cnt",
    "bene_race_othr_cnt",
    "bene_dual_cnt",
    "bene_ndual_cnt",
    "bene_cc_bh_adhd_othcd_v1_pct",
    "bene_cc_bh_alcohol_drug_v1_pct",
    "bene_cc_bh_tobacco_v1_pct",
    "bene_cc_bh_alz_nonalzdem_v2_pct",
    "bene_cc_bh_anxiety_v1_pct",
    "bene_cc_bh_bipolar_v1_pct",
    "bene_cc_bh_mood_v2_pct",
    "bene_cc_bh_depress_v1_pct",
    "bene_cc_bh_pd_v1_pct",
    "bene_cc_bh_ptsd_v1_pct",
    "bene_cc_bh_schizo_othpsy_v1_pct",
    "bene_cc_ph_asthma_v2_pct",
    "bene_cc_ph_afib_v2_pct",
    "bene_cc_ph_cancer6_v2_pct",
    "bene_cc_ph_ckd_v2_pct",
    "bene_cc_ph_copd_v2_pct",
    "bene_cc_ph_diabetes_v2_pct",
    "bene_cc_ph_hf_nonihd_v2_pct",
    "bene_cc_ph_hyperlipidemia_v2_pct",
    "bene_cc_ph_hypertension_v2_pct",
    "bene_cc_ph_ischemicheart_v2_pct",
    "bene_cc_ph_osteoporosis_v2_pct",
    "bene_cc_ph_parkinson_v2_pct",
    "bene_cc_ph_arthritis_v2_pct",
    "bene_cc_ph_stroke_tia_v2_pct",
    "bene_avg_risk_scre",
)
MUP_DRG_COLUMNS = (
    "rndrng_prvdr_ccn",
    "rndrng_prvdr_org_name",
    "rndrng_prvdr_city",
    "rndrng_prvdr_st",
    "rndrng_prvdr_state_fips",
    "rndrng_prvdr_zip5",
    "rndrng_prvdr_state_abrvtn",
    "rndrng_prvdr_ruca",
    "rndrng_prvdr_ruca_desc",
    "drg_cd",
    "drg_desc",
    "tot_dschrgs",
    "avg_submtd_cvrd_chrg",
    "avg_tot_pymt_amt",
    "avg_mdcr_pymt_amt",
)
# The bronze columns of the owner, enrollment and change-of-ownership tables, without the provenance columns.
OWNER_COLUMNS = (
    "enrollment_id",
    "associate_id",
    "organization_name",
    "associate_id_owner",
    "type_owner",
    "role_code_owner",
    "role_text_owner",
    "association_date_owner",
    "organization_name_owner",
    "doing_business_as_name_owner",
    "state_owner",
    "percentage_ownership",
    "created_for_acquisition_owner",
    "corporation_owner",
    "llc_owner",
    "medical_provider_supplier_owner",
    "management_services_company_owner",
    "medical_staffing_company_owner",
    "holding_company_owner",
    "investment_firm_owner",
    "financial_institution_owner",
    "consulting_firm_owner",
    "for_profit_owner",
    "non_profit_owner",
    "other_type_owner",
    "other_type_text_owner",
    "private_equity_company_owner",
    "reit_owner",
    "chain_home_office_owner",
    "owned_by_another_org_or_ind_owner",
)
ENROLLMENT_COLUMNS = (
    "enrollment_id",
    "enrollment_state",
    "provider_type_code",
    "provider_type_text",
    "npi",
    "multiple_npi_flag",
    "ccn",
    "associate_id",
    "organization_name",
    "doing_business_as_name",
    "incorporation_date",
    "incorporation_state",
    "organization_type_structure",
    "organization_other_type_text",
    "proprietary_nonprofit",
    "address_line_1",
    "address_line_2",
    "city",
    "state",
    "zip_code",
    "practice_location_type",
    "location_other_type_text",
    "subgroup_general",
    "subgroup_acute_care",
    "subgroup_alcohol_drug",
    "subgroup_childrens",
    "subgroup_long_term",
    "subgroup_psychiatric",
    "subgroup_rehabilitation",
    "subgroup_short_term",
    "subgroup_swing_bed_approved",
    "subgroup_psychiatric_unit",
    "subgroup_rehabilitation_unit",
    "subgroup_specialty_hospital",
    "subgroup_other",
    "subgroup_other_text",
    "reh_conversion_flag",
    "reh_conversion_date",
    "cah_or_hospital_ccn",
)
CHOW_COLUMNS = (
    "enrollment_id_buyer",
    "enrollment_state_buyer",
    "provider_type_code_buyer",
    "provider_type_text_buyer",
    "npi_buyer",
    "multiple_npi_flag_buyer",
    "ccn_buyer",
    "associate_id_buyer",
    "organization_name_buyer",
    "doing_business_as_name_buyer",
    "chow_type_code",
    "chow_type_text",
    "effective_date",
    "enrollment_id_seller",
    "enrollment_state_seller",
    "provider_type_code_seller",
    "provider_type_text_seller",
    "npi_seller",
    "multiple_npi_flag_seller",
    "ccn_seller",
    "associate_id_seller",
    "organization_name_seller",
    "doing_business_as_name_seller",
)
# The bronze columns of the HHS capacity and ONC tables, without the provenance columns.
HHS_COLUMNS = (
    "hospital_pk",
    "collection_week",
    "state",
    "ccn",
    "hospital_name",
    "address",
    "city",
    "zip",
    "hospital_subtype",
    "fips_code",
    "is_metro_micro",
    "total_beds_7_day_avg",
    "all_adult_hospital_beds_7_day_avg",
    "all_adult_hospital_inpatient_beds_7_day_avg",
    "inpatient_beds_used_7_day_avg",
    "all_adult_hospital_inpatient_bed_occupied_7_day_avg",
    "inpatient_beds_used_covid_7_day_avg",
    "total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg",
    "total_adult_patients_hospitalized_confirmed_covid_7_day_avg",
    "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg",
    "total_pediatric_patients_hospitalized_confirmed_covid_7_day_avg",
    "inpatient_beds_7_day_avg",
    "total_icu_beds_7_day_avg",
    "total_staffed_adult_icu_beds_7_day_avg",
    "icu_beds_used_7_day_avg",
    "staffed_adult_icu_bed_occupancy_7_day_avg",
    "staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_avg",
    "staffed_icu_adult_patients_confirmed_covid_7_day_avg",
    "total_patients_hospitalized_confirmed_influenza_7_day_avg",
    "icu_patients_confirmed_influenza_7_day_avg",
    "total_patients_hospitalized_confirmed_influenza_and_covid_7_day_avg",
    "total_beds_7_day_sum",
    "all_adult_hospital_beds_7_day_sum",
    "all_adult_hospital_inpatient_beds_7_day_sum",
    "inpatient_beds_used_7_day_sum",
    "all_adult_hospital_inpatient_bed_occupied_7_day_sum",
    "inpatient_beds_used_covid_7_day_sum",
    "total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum",
    "total_adult_patients_hospitalized_confirmed_covid_7_day_sum",
    "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum",
    "total_pediatric_patients_hospitalized_confirmed_covid_7_day_sum",
    "inpatient_beds_7_day_sum",
    "total_icu_beds_7_day_sum",
    "total_staffed_adult_icu_beds_7_day_sum",
    "icu_beds_used_7_day_sum",
    "staffed_adult_icu_bed_occupancy_7_day_sum",
    "staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum",
    "staffed_icu_adult_patients_confirmed_covid_7_day_sum",
    "total_patients_hospitalized_confirmed_influenza_7_day_sum",
    "icu_patients_confirmed_influenza_7_day_sum",
    "total_patients_hospitalized_confirmed_influenza_and_covid_7_day_sum",
    "total_beds_7_day_coverage",
    "all_adult_hospital_beds_7_day_coverage",
    "all_adult_hospital_inpatient_beds_7_day_coverage",
    "inpatient_beds_used_7_day_coverage",
    "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
    "inpatient_beds_used_covid_7_day_coverage",
    "total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage",
    "total_adult_patients_hospitalized_confirmed_covid_7_day_coverage",
    "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage",
    "total_pediatric_patients_hospitalized_confirmed_covid_7_day_coverage",
    "inpatient_beds_7_day_coverage",
    "total_icu_beds_7_day_coverage",
    "total_staffed_adult_icu_beds_7_day_coverage",
    "icu_beds_used_7_day_coverage",
    "staffed_adult_icu_bed_occupancy_7_day_coverage",
    "staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage",
    "staffed_icu_adult_patients_confirmed_covid_7_day_coverage",
    "total_patients_hospitalized_confirmed_influenza_7_day_coverage",
    "icu_patients_confirmed_influenza_7_day_coverage",
    "total_patients_hospitalized_confirmed_influenza_and_covid_7_day_coverage",
    "previous_day_admission_adult_covid_confirmed_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_18_19_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_20_29_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_30_39_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_40_49_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_50_59_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_60_69_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_70_79_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_80_7_day_sum",
    "previous_day_admission_adult_covid_confirmed_unknown_7_day_sum",
    "previous_day_admission_pediatric_covid_confirmed_7_day_sum",
    "previous_day_covid_ed_visits_7_day_sum",
    "previous_day_admission_adult_covid_suspected_7_day_sum",
    "previous_day_admission_adult_covid_suspected_18_19_7_day_sum",
    "previous_day_admission_adult_covid_suspected_20_29_7_day_sum",
    "previous_day_admission_adult_covid_suspected_30_39_7_day_sum",
    "previous_day_admission_adult_covid_suspected_40_49_7_day_sum",
    "previous_day_admission_adult_covid_suspected_50_59_7_day_sum",
    "previous_day_admission_adult_covid_suspected_60_69_7_day_sum",
    "previous_day_admission_adult_covid_suspected_70_79_7_day_sum",
    "previous_day_admission_adult_covid_suspected_80_7_day_sum",
    "previous_day_admission_adult_covid_suspected_unknown_7_day_sum",
    "previous_day_admission_pediatric_covid_suspected_7_day_sum",
    "previous_day_total_ed_visits_7_day_sum",
    "previous_day_admission_influenza_confirmed_7_day_sum",
    "geocoded_hospital_address",
    "hhs_ids",
    "previous_day_admission_adult_covid_confirmed_7_day_coverage",
    "previous_day_admission_pediatric_covid_confirmed_7_day_coverage",
    "previous_day_admission_adult_covid_suspected_7_day_coverage",
    "previous_day_admission_pediatric_covid_suspected_7_day_coverage",
    "previous_week_personnel_covid_vaccinated_doses_administered_7_day",
    "total_personnel_covid_vaccinated_doses_none_7_day",
    "total_personnel_covid_vaccinated_doses_one_7_day",
    "total_personnel_covid_vaccinated_doses_all_7_day",
    "previous_week_patients_covid_vaccinated_doses_one_7_day",
    "previous_week_patients_covid_vaccinated_doses_all_7_day",
    "is_corrected",
    "all_pediatric_inpatient_bed_occupied_7_day_avg",
    "all_pediatric_inpatient_bed_occupied_7_day_coverage",
    "all_pediatric_inpatient_bed_occupied_7_day_sum",
    "all_pediatric_inpatient_beds_7_day_avg",
    "all_pediatric_inpatient_beds_7_day_coverage",
    "all_pediatric_inpatient_beds_7_day_sum",
    "previous_day_admission_pediatric_covid_confirmed_0_4_7_day_sum",
    "previous_day_admission_pediatric_covid_confirmed_12_17_7_day_sum",
    "previous_day_admission_pediatric_covid_confirmed_5_11_7_day_sum",
    "previous_day_admission_pediatric_covid_confirmed_unknown_7_day_sum",
    "staffed_icu_pediatric_patients_confirmed_covid_7_day_avg",
    "staffed_icu_pediatric_patients_confirmed_covid_7_day_coverage",
    "staffed_icu_pediatric_patients_confirmed_covid_7_day_sum",
    "staffed_pediatric_icu_bed_occupancy_7_day_avg",
    "staffed_pediatric_icu_bed_occupancy_7_day_coverage",
    "staffed_pediatric_icu_bed_occupancy_7_day_sum",
    "total_staffed_pediatric_icu_beds_7_day_avg",
    "total_staffed_pediatric_icu_beds_7_day_coverage",
    "total_staffed_pediatric_icu_beds_7_day_sum",
)
ONC_CHPL_COLUMNS = (
    "facility_id",
    "facility_name",
    "address",
    "city_town",
    "state",
    "zip_code",
    "county_parish",
    "telephone_number",
    "meets_criteria_for_promoting_interoperability_of_ehrs",
    "start_date",
    "end_date",
    "cehrt_id",
    "chpl_id",
    "product_database_id",
    "developer_name",
    "product_name",
    "year",
)
ONC_ATTESTATION_COLUMNS = (
    "npi",
    "ccn",
    "provider_type",
    "business_state_territory",
    "zip",
    "hospital_type",
    "program_type",
    "program_year",
    "provider_stage_number",
    "payment_year",
    "attestation_month",
    "attestation_year",
    "mu_definition_year",
    "stage_2_scheduled_2014",
    "ehr_certification_number",
    "ehr_product_chp_id",
    "vendor_name",
    "ehr_product_name",
    "ehr_product_version",
    "product_classification",
    "product_setting",
    "product_certification_edition_yr",
)
# The C1 geography tables' bronze columns [400] to [419].
HUD_COLUMNS = ("zip", "geoid", "res_ratio", "bus_ratio", "oth_ratio", "tot_ratio", "state", "city")
ADJACENCY_COLUMNS = ("county_name", "county_geoid", "neighbor_name", "neighbor_geoid", "length")
RUCC_COLUMNS = ("fips", "state", "county_name", "attribute", "value")
RUCA_TRACT_COLUMNS = (
    "tractfips23",
    "countyfips23",
    "countycode23",
    "countyname23",
    "tractfips20",
    "tractcode20",
    "tractname20",
    "countyfips20",
    "countycode20",
    "countyname20",
    "statefips20",
    "statename20",
    "urbanareacode20",
    "urbanareaname20",
    "urbancore",
    "urbancoretype",
    "primaryruca",
    "primaryrucadescription",
    "primarydestinationcode",
    "primarydestinationname",
    "secondaryruca",
    "secondaryrucadescription",
    "secondarydestinationcode",
    "secondarydestinationname",
    "population",
    "landarea",
    "popdensity",
)
RUCA_ZIP_2020_COLUMNS = ("zipcode", "state", "zipcodetype", "poname", "primaryruca", "secondaryruca")
RUCA_ZIP_2010_COLUMNS = ("zip_code", "state", "zip_type", "ruca1", "ruca2")
HSA_COLUMNS = ("medicare_prov_num", "zip_cd_of_residence", "total_days_of_care", "total_charges", "total_cases")
# The C2 tables' bronze columns used by the fixture: SVI identifiers and a few fields of two editions, and the ACS columns
# of the fixture's variable map; every other ACS table has only its geography columns [420] to [434].
SVI_COLUMNS = (
    "st",
    "state",
    "st_abbr",
    "stcnty",
    "county",
    "fips",
    "location",
    "state_fips",
    "cnty_fips",
    "stcofips",
    "state_name",
    "state_abbr",
    "shape",
    "shape_starea",
    "shape_stlength",
    "affgeoid",
    "e_totpop",
    "ep_pov150",
    "ep_unemp",
    "rpl_themes",
    "g1v1r",
)
SAHIE_COLUMNS = (
    "year",
    "version",
    "statefips",
    "countyfips",
    "geocat",
    "agecat",
    "racecat",
    "sexcat",
    "iprcat",
    "nipr",
    "nipr_moe",
    "nui",
    "nui_moe",
    "nic",
    "nic_moe",
    "pctui",
    "pctui_moe",
    "pctic",
    "pctic_moe",
    "pctelig",
    "pctelig_moe",
    "pctliic",
    "pctliic_moe",
    "state_name",
    "county_name",
)
PLACES_COLUMNS = (
    "year",
    "stateabbr",
    "locationname",
    "locationid",
    "measureid",
    "datavaluetypeid",
    "data_value",
    "data_value_footnote_symbol",
    "low_confidence_limit",
    "high_confidence_limit",
    "totalpopulation",
)
GV_COLUMNS = (
    "year",
    "bene_geo_lvl",
    "bene_geo_desc",
    "bene_geo_cd",
    "bene_age_lvl",
    "benes_total_cnt",
    "ma_prtcptn_rate",
    "bene_dual_pct",
    "pqi03_dbts_age_65_74",
)
WONDER_COLUMNS = (
    "notes",
    "county",
    "county_code",
    "year",
    "year_code",
    "deaths",
    "population",
    "crude_rate",
    "crude_rate_lower_95_confidence_interval",
    "crude_rate_upper_95_confidence_interval",
    "crude_rate_standard_error",
)
MMD_COLUMNS = ("year", "geography", "domain", "condition", "fips", "county", "state", "urban", "primary_denominator", "analysis_value")
HPSA_COLUMNS = (
    "hpsa_name",
    "hpsa_id",
    "designation_type",
    "hpsa_discipline_class",
    "hpsa_score",
    "hpsa_status",
    "hpsa_designation_date",
    "hpsa_designation_last_update_date",
    "withdrawn_date",
    "hpsa_geography_identification_number",
    "hpsa_component_type_description",
    "state_and_county_federal_information_processing_standard_code",
    "common_state_county_fips_code",
    "state_fips_code",
    "rural_status",
    "hpsa_postal_code",
    "primary_state_abbreviation",
)
MUA_COLUMNS = (
    "mua_p_id",
    "designation_type_code",
    "designation_type",
    "mua_p_status_description",
    "designation_date",
    "mua_p_update_date",
    "medically_underserved_area_population_mua_p_withdrawal_date",
    "imu_score",
    "population_type",
    "medically_underserved_area_population_mua_p_component_geographic_name",
    "medically_underserved_area_population_mua_p_component_geographic_type_description",
    "state_and_county_federal_information_processing_standard_code",
    "county_subdivision_fips_code",
    "state_fips_code",
    "rural_status_description",
)
BLS_COLUMNS = ("seriesid", "county_fips", "measure_code", "year", "period", "periodname", "value", "footnotes")
D1_WINDOW_COLUMNS = (
    "facility_id",
    "provider_id",
    "measure_id",
    "measure_name",
    "compared_to_national",
    "denominator",
    "score",
    "lower_estimate",
    "higher_estimate",
    "footnote",
    "start_date",
    "end_date",
    "measure_start_date",
    "measure_end_date",
)
ACS_COLUMNS = {
    "acs_dp04": ("geo_id", "name", "dp04_0077pe", "dp04_0078pe"),
    "acs_s1701": ("geo_id", "name", "s1701_c03_001e"),
    "acs_b19013": ("geo_id", "name", "b19013_001e", "b19013_001m"),
    "acs_summary_b19013": ("geo_id", "b19013_001e", "b19013_001m", "b19013_e001", "b19013_m001"),
}
WIDE_COLUMNS = {
    "cms_medicare_inpatient_by_provider": MUP_PROVIDER_COLUMNS,
    "cms_medicare_inpatient_by_drg": MUP_DRG_COLUMNS,
    "cms_hospital_cost_reports": COST_REPORT_COLUMNS,
    "cms_provider_of_services": (
        "prvdr_num",
        "prvdr_ctgry_cd",
        "prvdr_ctgry_sbtyp_cd",
        "fac_name",
        "state_cd",
        "fips_state_cd",
        "fips_cnty_cd",
        "ssa_state_cd",
        "ssa_cnty_cd",
        "zip_cd",
        "cbsa_cd",
        "cbsa_urbn_rrl_ind",
        "gnrl_cntl_type_cd",
        "pgm_trmntn_cd",
        "trmntn_exprtn_dt",
        "crtfctn_dt",
        "orgnl_prtcptn_dt",
        "mdcl_schl_afltn_cd",
        "dctd_er_srvc_cd",
        "icu_srvc_cd",
        "srgcl_icu_srvc_cd",
        "neontl_icu_srvc_cd",
        "ped_icu_srvc_cd",
        "burn_care_unit_srvc_cd",
        "acute_rnl_dlys_srvc_cd",
        "ip_srgcl_srvc_cd",
        "open_hrt_srgry_srvc_cd",
        "rsdnt_pgm_alpthc_sw",
        "rsdnt_pgm_dntl_sw",
        "rsdnt_pgm_ostpthc_sw",
        "rsdnt_pgm_othr_sw",
        "rsdnt_pgm_pdtrc_sw",
        "bed_cnt",
        "crtfd_bed_cnt",
        "psych_unit_bed_cnt",
        "rehab_unit_bed_cnt",
        "oprtg_room_cnt",
        "endscpy_prcdr_rooms_cnt",
        "crdc_cthrtztn_prcdr_rooms_cnt",
        "tot_ofsite_emer_dept_cnt",
        "rn_cnt",
        "lpn_lvn_cnt",
        "nrs_prctnr_cnt",
        "crna_cnt",
    ),
    "cms_cc_hospital_general_information": (
        "facility_id",
        "provider_id",
        "state",
        "county_name",
        "county_parish",
        "zip_code",
        "hospital_type",
        "hospital_ownership",
        "emergency_services",
        "hospital_overall_rating",
        "hospital_overall_rating_footnote",
    ),
    "cms_cc_timely_and_effective_care_hospital": (
        "facility_id",
        "provider_id",
        "condition",
        "measure_id",
        "measure_name",
        "score",
        "sample",
        "footnote",
        "start_date",
        "end_date",
        "measure_start_date",
        "measure_end_date",
    ),
    # Group D1: visits and deaths in the HAI-like layout with the older provider_id columns; HRRP keyed by measure_name [505] [506].
    "cms_cc_unplanned_hospital_visits_hospital": D1_WINDOW_COLUMNS + ("number_of_patients", "number_of_patients_returned"),
    "cms_cc_complications_and_deaths_hospital": D1_WINDOW_COLUMNS,
    "cms_cc_hospital_readmissions_reduction_program_hospital": (
        "facility_id",
        "measure_name",
        "number_of_discharges",
        "footnote",
        "excess_readmission_ratio",
        "predicted_readmission_rate",
        "expected_readmission_rate",
        "number_of_readmissions",
        "start_date",
        "end_date",
    ),
    # Group D2: HAC with both PSI-90 column names and a republished SIR; TPS with and without fiscal_year [516] [517].
    "cms_cc_hac_reduction_program_hospital": (
        "facility_id",
        "fiscal_year",
        "psi_90_composite_value",
        "psi_90_composite",
        "psi_90_w_z_score",
        "psi_90_start_date",
        "psi_90_end_date",
        "clabsi_sir",
        "clabsi_w_z_score",
        "cauti_w_z_score",
        "ssi_w_z_score",
        "cdi_w_z_score",
        "mrsa_w_z_score",
        "hai_measures_start_date",
        "hai_measures_end_date",
        "total_hac_score",
        "total_hac_score_footnote",
        "total_hac_footnote",
        "payment_reduction",
        "payment_reduction_footnote",
    ),
    "cms_cc_hvbp_tps": (
        "fiscal_year",
        "facility_id",
        "provider_number",
        "unweighted_normalized_clinical_care_domain_score",
        "weighted_normalized_clinical_care_domain_score",
        "unweighted_normalized_clinical_outcomes_domain_score",
        "weighted_normalized_clinical_outcomes_domain_score",
        "unweighted_person_and_community_engagement_domain_score",
        "weighted_person_and_community_engagement_domain_score",
        "unweighted_normalized_safety_domain_score",
        "weighted_safety_domain_score",
        "unweighted_normalized_efficiency_and_cost_reduction_domain_score",
        "weighted_efficiency_and_cost_reduction_domain_score",
        "total_performance_score",
    ),
    # The maternal table has no provider_id and no measure_start_date [319] [320].
    "cms_cc_maternal_health_hospital": ("facility_id", "measure_id", "measure_name", "score", "sample", "footnote", "start_date", "end_date"),
    "cms_cc_hcahps_hospital": (
        "facility_id",
        "provider_id",
        "hcahps_measure_id",
        "hcahps_question",
        "hcahps_answer_description",
        "patient_survey_star_rating",
        "patient_survey_star_rating_footnote",
        "hcahps_answer_percent",
        "hcahps_answer_percent_footnote",
        "hcahps_linear_mean_value",
        "number_of_completed_surveys",
        "number_of_completed_surveys_footnote",
        "survey_response_rate_percent",
        "survey_response_rate_percent_footnote",
        "start_date",
        "end_date",
        "measure_start_date",
        "measure_end_date",
    ),
    "cms_hospital_enrollments": ENROLLMENT_COLUMNS,
    "cms_hospital_owners": OWNER_COLUMNS,
    "cms_change_of_ownership": CHOW_COLUMNS,
    "hhs_capacity_csv": HHS_COLUMNS,
    "onc_pi_chpl_linkage_csv": ONC_CHPL_COLUMNS,
    "onc_pi_attestations_csv": ONC_ATTESTATION_COLUMNS,
    "hud_zip_county": HUD_COLUMNS,
    "county_adjacency": ADJACENCY_COLUMNS,
    "rucc": RUCC_COLUMNS,
    "ruca_tracts_2020": RUCA_TRACT_COLUMNS,
    "ruca_zip_2020": RUCA_ZIP_2020_COLUMNS,
    "ruca_zip_2010": RUCA_ZIP_2010_COLUMNS,
    "cms_hsa_csv": HSA_COLUMNS,
    "svi": SVI_COLUMNS,
    "sahie": SAHIE_COLUMNS,
    "bls_laus": BLS_COLUMNS,
    "places": PLACES_COLUMNS,
    "cms_geographic_variation_csv": GV_COLUMNS,
    "wonder_county_mortality": WONDER_COLUMNS,
    "cms_mmd_csv": MMD_COLUMNS,
    "hrsa_hpsa_detail": HPSA_COLUMNS,
    "hrsa_mua_detail": MUA_COLUMNS,
    "acs_dp02": ACS_COLUMNS.get("acs_dp02", ("geo_id", "name")),
    "acs_dp03": ACS_COLUMNS.get("acs_dp03", ("geo_id", "name")),
    "acs_dp04": ACS_COLUMNS.get("acs_dp04", ("geo_id", "name")),
    "acs_dp05": ACS_COLUMNS.get("acs_dp05", ("geo_id", "name")),
    "acs_s0101": ACS_COLUMNS.get("acs_s0101", ("geo_id", "name")),
    "acs_s0601": ACS_COLUMNS.get("acs_s0601", ("geo_id", "name")),
    "acs_s1701": ACS_COLUMNS.get("acs_s1701", ("geo_id", "name")),
    "acs_s2503": ACS_COLUMNS.get("acs_s2503", ("geo_id", "name")),
    "acs_s2701": ACS_COLUMNS.get("acs_s2701", ("geo_id", "name")),
    "acs_b16005": ACS_COLUMNS.get("acs_b16005", ("geo_id", "name")),
    "acs_b19013": ACS_COLUMNS.get("acs_b19013", ("geo_id", "name")),
    "acs_b25070": ACS_COLUMNS.get("acs_b25070", ("geo_id", "name")),
    "acs_b25091": ACS_COLUMNS.get("acs_b25091", ("geo_id", "name")),
    "acs_b26001": ACS_COLUMNS.get("acs_b26001", ("geo_id", "name")),
    "acs_c16001": ACS_COLUMNS.get("acs_c16001", ("geo_id", "name")),
    "acs_summary_b16005": ACS_COLUMNS.get("acs_summary_b16005", ("geo_id",)),
    "acs_summary_b19013": ACS_COLUMNS.get("acs_summary_b19013", ("geo_id",)),
    "acs_summary_b25070": ACS_COLUMNS.get("acs_summary_b25070", ("geo_id",)),
    "acs_summary_b25091": ACS_COLUMNS.get("acs_summary_b25091", ("geo_id",)),
    "acs_summary_b26001": ACS_COLUMNS.get("acs_summary_b26001", ("geo_id",)),
    "acs_summary_c16001": ACS_COLUMNS.get("acs_summary_c16001", ("geo_id",)),
}


def pos(ccn: str, category: str = "01", **fields: str) -> tuple[tuple[str, str], ...]:
    """Return one POS row: the CCN, the provider category and any other bronze columns."""
    return (("prvdr_num", ccn), ("prvdr_ctgry_cd", category), *fields.items())


# Two POS snapshots. Codes with and without leading zeros, a county from short parts, a non-hospital category, a Veterans
# Health Administration CCN, a critical access CCN, a Canadian row with no county, Connecticut, a blank count, switches as
# Y/N and true/false, and a terminated hospital [282] to [292].
POS_DEC18 = (
    pos(
        "010001",
        prvdr_ctgry_sbtyp_cd="1",
        state_cd="AL",
        fips_state_cd="1",
        fips_cnty_cd="73",
        ssa_state_cd="01",
        ssa_cnty_cd="360",
        zip_cd="35233",
        gnrl_cntl_type_cd="4",
        pgm_trmntn_cd="00",
        bed_cnt="250",
        crtfd_bed_cnt="240",
        rn_cnt="100.5",
        rsdnt_pgm_alpthc_sw="Y",
        rsdnt_pgm_dntl_sw="N",
        icu_srvc_cd="1",
        orgnl_prtcptn_dt="19660701",
    ),
    pos("01001F", state_cd="AL", fips_state_cd="01", fips_cnty_cd="001", gnrl_cntl_type_cd="10", pgm_trmntn_cd="00", bed_cnt="100"),
    pos("011301", prvdr_ctgry_sbtyp_cd="11", state_cd="AL", fips_state_cd="01", fips_cnty_cd="003", pgm_trmntn_cd="00", bed_cnt="25"),
    pos("010100", category="02", state_cd="AL", pgm_trmntn_cd="00"),
    pos("990001", state_cd="CN", fips_state_cd="", fips_cnty_cd="", pgm_trmntn_cd="00"),
    pos("070001", state_cd="CT", fips_state_cd="09", fips_cnty_cd="001", pgm_trmntn_cd="00"),
    # [616] No county but a ZIP that HUD splits between two counties.
    pos("010002", state_cd="DC", zip_cd="20001", pgm_trmntn_cd="00", bed_cnt=" "),
)
POS_MAR19 = (
    pos(
        "010001",
        prvdr_ctgry_sbtyp_cd="01",
        state_cd="AL",
        fips_state_cd="01",
        fips_cnty_cd="073",
        pgm_trmntn_cd="00",
        bed_cnt="255",
        rn_cnt="101",
        rsdnt_pgm_alpthc_sw="true",
        rsdnt_pgm_dntl_sw="false",
    ),
    pos("010002", state_cd="AL", pgm_trmntn_cd="01", trmntn_exprtn_dt="20190115", bed_cnt="50"),
)
# The snapshot before the 2021 window: a critical access, a Veterans Health Administration, a Connecticut, a Canadian and a
# Maryland row; 010009 has HAI rows and a CMI but no POS row [309] [313] [314] [316] [317].
POS_DEC20 = (
    pos("010001", prvdr_ctgry_sbtyp_cd="01", state_cd="AL", fips_state_cd="01", fips_cnty_cd="073", pgm_trmntn_cd="00", bed_cnt="260"),
    pos("010005", prvdr_ctgry_sbtyp_cd="01", state_cd="AL", fips_state_cd="01", fips_cnty_cd="089", pgm_trmntn_cd="00", bed_cnt="120"),
    pos("011301", prvdr_ctgry_sbtyp_cd="11", state_cd="AL", fips_state_cd="01", fips_cnty_cd="003", pgm_trmntn_cd="00", bed_cnt="25"),
    pos("01001F", state_cd="AL", fips_state_cd="01", fips_cnty_cd="001", pgm_trmntn_cd="00", bed_cnt="100"),
    # [631] A Connecticut ZIP that HUD places in a planning region.
    pos("070001", prvdr_ctgry_sbtyp_cd="01", state_cd="CT", fips_state_cd="09", fips_cnty_cd="001", zip_cd="06001", pgm_trmntn_cd="00", bed_cnt="300"),
    # [616] A ZIP whose HUD rows are in another state stays without a county.
    pos("990001", state_cd="CN", zip_cd="35004", pgm_trmntn_cd="00"),
    pos("210001", prvdr_ctgry_sbtyp_cd="01", state_cd="MD", fips_state_cd="24", fips_cnty_cd="005", pgm_trmntn_cd="00", bed_cnt="200"),
    # [701] 010006's only snapshot ends 36 months after its 2018 window starts: too far to carry.
    pos("010006", prvdr_ctgry_sbtyp_cd="01", state_cd="AL", fips_state_cd="01", fips_cnty_cd="089", pgm_trmntn_cd="00", bed_cnt="80"),
)
# HAI rows for the 2021 calendar-year window, and one rolling window that is not a spine year [306].
HAI_SPINE = (
    "010001|HAI_1_SIR|01/01/2021|12/31/2021|0.4",
    "010005|HAI_1_SIR|01/01/2021|12/31/2021|0.6",
    "010005|HAI_1_SIR|04/01/2020|03/31/2021|0.7",
    "011301|HAI_1_SIR|01/01/2021|12/31/2021|0.5",
    "01001F|HAI_1_SIR|01/01/2021|12/31/2021|0.3",
    "070001|HAI_1_SIR|01/01/2021|12/31/2021|0.9",
    "990001|HAI_1_SIR|01/01/2021|12/31/2021|1.0",
    "010009|HAI_1_SIR|01/01/2021|12/31/2021|1.1",
    "210001|HAI_1_SIR|01/01/2021|12/31/2021|0.8",
    # [613] [614] 010005's 2019 window comes before its first snapshot; 011301's 2020 window falls between two snapshots.
    "010005|HAI_1_SIR|01/01/2019|12/31/2019|0.6",
    "011301|HAI_1_SIR|01/01/2020|12/31/2020|0.5",
    "010006|HAI_1_SIR|01/01/2018|12/31/2018|0.6",
)
# AL1 [549] to [556]: 070001's 2021 window with every HAI_1 part, HAI_2 published as counts without a SIR (a footnote with
# its text), HAI_3 and HAI_6 as other tokens; 210001's HAI_5 SIR from a release after the last 2015-baseline review is held.
HAI_OUTCOME = (
    "070001|HAI_1_CILOWER|01/01/2021|12/31/2021|0.5",
    "070001|HAI_1_CIUPPER|01/01/2021|12/31/2021|1.5",
    "070001|HAI_1_NUMERATOR|01/01/2021|12/31/2021|9|3",
    "070001|HAI_1_ELIGCASES|01/01/2021|12/31/2021|10.000",
    "070001|HAI_1_DOPC|01/01/2021|12/31/2021|1000",
    "070001|HAI_2_SIR|01/01/2021|12/31/2021|Not Available|13 - Results cannot be calculated for this reporting period.|Not Available",
    "070001|HAI_2_CILOWER|01/01/2021|12/31/2021|Not Available|13",
    "070001|HAI_2_CIUPPER|01/01/2021|12/31/2021|Not Available|13",
    "070001|HAI_2_NUMERATOR|01/01/2021|12/31/2021|0",
    "070001|HAI_2_ELIGCASES|01/01/2021|12/31/2021|0.412",
    "070001|HAI_2_DOPC|01/01/2021|12/31/2021|800",
    "070001|HAI_3_SIR|01/01/2021|12/31/2021|--|3, 13|No Different than National Benchmark",
    "070001|HAI_6_SIR|01/01/2021|12/31/2021|N/A|12",
)
GROUP_AL1 = (
    Stored("cms_hai_hospital", "h09", "2022-06-01", "HAI_Outcome_Fixture.csv", sha("a7"), len(HAI_OUTCOME), content=HAI_OUTCOME),
    Stored("cms_hai_hospital", "h10", "2026-09-01", "HAI_Late_Fixture.csv", sha("a8"), 1, content=("210001|HAI_5_SIR|01/01/2021|12/31/2021|0.7",)),
    Stored("cms_hai_hospital", "h11", "2022-06-01", "HAI_Conflict_A.csv", sha("a9"), 1, content=("990001|HAI_4_SIR|01/01/2021|12/31/2021|0.4",)),
    Stored("cms_hai_hospital", "h12", "2022-06-01", "HAI_Conflict_B.csv", sha("b9"), 1, content=("990001|HAI_4_SIR|01/01/2021|12/31/2021|0.6",)),
)
# Each POS file's catalog coverage, as the period seed gives it [280].
POS_PERIODS = {"pa": ("2018-10-01", "2018-12-31"), "pb": ("2019-01-01", "2019-03-31"), "pc": ("2020-10-01", "2020-12-31")}
# Each owner, enrollment and change-of-ownership file's release label and catalog period, as the receipts give them [365].
OWNERSHIP_PERIODS = {
    "e1": ("Hospital Enrollments : 2024-01-01", "2024-01-01", "2024-01-31"),
    "o1": ("Hospital All Owners : 2025-05-01", "2025-05-01", "2025-05-31"),
    "x1": ("Hospital Change of Ownership : 2023-12-01", "2023-10-01", "2023-12-31"),
    "ow1": ("Hospital All Owners : 2022-11-14", "2022-11-01", "2022-11-30"),
    "ow0": ("Hospital All Owners : 2020-07-01", "2020-07-01", "2020-07-31"),
    "ow2": ("Hospital All Owners : 2025-04-01", "2025-04-01", "2025-04-30"),
    "ow9": ("Hospital All Owners : 2025-06-01", "2025-06-01", "2025-06-30"),
    "en1": ("Hospital Enrollments : 2022-11-01", "2022-11-01", "2022-11-30"),
    "xw1": ("Hospital Change of Ownership : 2022-03-31", "2022-01-01", "2022-03-31"),
    "xw2": ("Hospital Change of Ownership : 2022-09-30", "2022-07-01", "2022-09-30"),
}
CMI_HEADER = "Provider No.\tCase Mix Index (CMI)\tTotal Cases\tTotal Relative Weights"
CMI_HEADER_2007 = "Provider Number\tSum of Relative Weights\tTransfer Adjusted Cases\tTransfer Adjusted CMI\tUnadjusted Cases\tUnadjusted CMI"
CMI_HEADER_2011 = "Provider ID\tCases\tTotal Case Mix\tCMI\tTransfer Adjusted Cases\tTransfer Adjusted Case Mix\tTransfer Adjusted CMI"
GROUP_A = (
    Stored("cms_provider_of_services", "pa", "CMS_POS__fixture_a", "POS_OTHER_DEC18.csv", sha("t1"), 7, "CMS_POS__fixture_a", records=POS_DEC18),
    Stored("cms_provider_of_services", "pb", "CMS_POS__fixture_b", "POS_OTHER_MAR19.csv", sha("t2"), 2, "CMS_POS__fixture_b", records=POS_MAR19),
    Stored(
        "cms_cc_hospital_general_information",
        "g1",
        "2024-01-31",
        "hospitals_2024-01-31.zip!Hospital_General_Information.csv",
        sha("u1"),
        2,
        records=(
            (
                ("facility_id", "010001"),
                ("state", "AL"),
                ("hospital_type", "Acute Care Hospitals"),
                ("hospital_ownership", "Proprietary"),
                ("emergency_services", "Yes"),
                ("hospital_overall_rating", "4"),
            ),
            (
                ("facility_id", "010005"),
                ("state", "AL"),
                ("hospital_type", "Critical Access Hospitals"),
                ("emergency_services", "No"),
                ("hospital_overall_rating", "Not Available"),
                ("hospital_overall_rating_footnote", "16"),
            ),
        ),
    ),
    Stored(
        "cms_hospital_enrollments",
        "e1",
        "ENROLL__fixture",
        "Hospital_Enrollments_2024.01.05.csv",
        sha("u2"),
        1,
        records=((("ccn", "010001"), ("enrollment_id", "O20000000001")),),
    ),
    Stored(
        "cms_hospital_owners",
        "o1",
        "CMS_OWNERS__fixture",
        "organisation_owners.csv",
        sha("u3"),
        1,
        records=((("enrollment_id", "O20000000001"), ("type_owner", "O"), ("private_equity_company_owner", "N")),),
    ),
    Stored(
        "cms_change_of_ownership",
        "x1",
        "CMS_CHOW__fixture",
        "Hospital_CHOW_2024.01.05.csv",
        sha("u4"),
        1,
        records=((("ccn_buyer", "010001"), ("ccn_seller", "010001"), ("effective_date", "01/01/2023")),),
    ),
    # CMI: quoted thousands and a blank footer line in the current layout [296] [298]; a headerless fixed-width proposed
    # file whose extra CCN gets no CMI because the year has a final file [295] [301].
    Stored(
        "cms_ipps_text_lines",
        "q1",
        "main-cmi-ipps__q1",
        "FY18 CMIs - V35 Billed DRGs (FR 2020).txt",
        sha("q1"),
        4,
        "main-cmi-ipps__q1",
        content=(CMI_HEADER, '010001\t1.9186\t"7,072"\t"13,568.39"', '010005\t1.3810\t"3,140"\t"4,336.49"', ""),
    ),
    Stored(
        "cms_ipps_text_lines",
        "q2",
        "main-cmi-ipps__q2",
        "FY18 CMIs - V35 Billed DRGs (NPRM 2020).txt",
        sha("q2"),
        2,
        "main-cmi-ipps__q2",
        content=("010001 07042 01.9188 13511.8844", "010007 01000 01.2000 01200.0000"),
    ),
    # A correction notice supersedes the final file of its rule year, CCN by file, not by row [301].
    Stored(
        "cms_ipps_text_lines",
        "q3",
        "main-cmi-ipps__q3",
        "FY23 CMIs - V40 Billed DRGs (CN 2025).txt",
        sha("q3"),
        2,
        "main-cmi-ipps__q3",
        content=(CMI_HEADER, "010001\t2.0542\t4209\t8646.1878"),
    ),
    Stored(
        "cms_ipps_text_lines",
        "q4",
        "main-cmi-ipps__q4",
        "FY23 CMIs - V40 Billed DRGs (FR 2025).txt",
        sha("q4"),
        3,
        "main-cmi-ipps__q4",
        content=(CMI_HEADER, '010001\t2.0540\t"4,209"\t"8,645.29"', '010005\t1.5456\t"1,479"\t"2,285.89"'),
    ),
    # Transfer-adjusted columns beside the unadjusted ones: the unadjusted CMI is read [294].
    Stored(
        "cms_ipps_text_lines",
        "q5",
        "main-cmi-ipps__q5",
        "CMIs FN07 Sept.txt",
        sha("q5"),
        2,
        "main-cmi-ipps__q5",
        content=(CMI_HEADER_2007, "010001\t15192.4568\t10032.7312\t1.4815\t10147\t1.4972"),
    ),
    # Two final files of one rule year: a CCN whose CMIs differ, or whose data years differ, is held [301] [302].
    Stored(
        "cms_ipps_text_lines",
        "q6",
        "main-cmi-ipps__q6",
        "Provider_CMI_V28_Final.txt",
        sha("q6"),
        4,
        "main-cmi-ipps__q6",
        content=(
            CMI_HEADER_2011,
            "010001\t8178\t13823.4333\t1.690319553\t8086.135317\t13532.49007\t1.673542371",
            "010005\t2232\t2830.1339\t1.267981138\t2199.355254\t2772.443294\t1.260570928",
            "010006\t5000\t8000\t1.6\t4950\t7900\t1.59596",
        ),
    ),
    Stored(
        "cms_ipps_text_lines",
        "q7",
        "main-cmi-ipps__q7",
        "FY19 CMIs - V36 Billed DRGs (FR 2021).txt",
        sha("q7"),
        3,
        "main-cmi-ipps__q7",
        content=(CMI_HEADER, '010001\t1.9837\t"6,752"\t"13,394.11"', "010006\t1.6000\t5000\t8000.00"),
    ),
    # A workbook-only file with a CCN that lost its leading zero [304]; a non-CMI file in a CMI snapshot [293]; a year whose
    # only file has no rule stage.
    Stored(
        "cms_ipps_sheet_rows",
        "q8",
        "main-cmi-ipps__q8",
        "FY25 CMIs - V42 Billed DRGs (FR 2027).xls",
        sha("q8"),
        2,
        "main-cmi-ipps__q8",
        content=("MPR CMIs:Provider No.|Case Mix Index (CMI)|Total Cases|Total Relative Weights", "MPR CMIs:10001|1.8688|4524.0|8454.4376"),
    ),
    Stored(
        "cms_ipps_text_lines",
        "q9",
        "main-cmi-ipps__q9",
        "FY26_January_PUF.20250131.OccMix Data PUF.txt",
        sha("q9"),
        2,
        "main-cmi-ipps__q9",
        content=("PROV\tMAC", "010001\t10001"),
    ),
    Stored("cms_ipps_text_lines", "r1", "main-cmi-ipps__r1", "CMIF11.txt", sha("r1"), 1, "main-cmi-ipps__r1", content=("010001 07913 01.751124 13856.646",)),
    # The spine: a POS snapshot before the 2021 window, its HAI rows, the FY 2020 data year published only in a proposed rule
    # (where 010005 is listed twice with different CMIs, so it is held) and the FY 2021 data year of the same rule's final file;
    # a data year in two rules takes the later rule [311] [312] [315].
    Stored("cms_provider_of_services", "pc", "CMS_POS__fixture_c", "POS_OTHER_DEC20.csv", sha("t3"), len(POS_DEC20), "CMS_POS__fixture_c", records=POS_DEC20),
    Stored("cms_hai_hospital", "h08", "2022-06-01", "HAI_Spine_Fixture.csv", sha("a6"), len(HAI_SPINE), content=HAI_SPINE),
    Stored(
        "cms_ipps_text_lines",
        "s1",
        "main-cmi-ipps__s1",
        "FY20 CMIs - V37 Billed DRGs (PR 2023).txt",
        sha("s1"),
        7,
        "main-cmi-ipps__s1",
        content=(
            CMI_HEADER,
            "010001\t2.0352\t4818\t9805.71",
            "010005\t1.6864\t1950\t3288.40",
            "070001\t1.5000\t1000\t1500.00",
            "010009\t1.2000\t500\t600.00",
            "010005\t1.7000\t2000\t3400.00",
            "210001\t1.8000\t1000\t1800.00",
        ),
    ),
    Stored(
        "cms_ipps_text_lines",
        "s2",
        "main-cmi-ipps__s2",
        "FY21 CMIs - V38 Billed DRGs (FR 2023).txt",
        sha("s2"),
        2,
        "main-cmi-ipps__s2",
        content=(CMI_HEADER, '010001\t2.0375\t"4,837"\t"9,855.42"'),
    ),
    Stored(
        "cms_ipps_text_lines",
        "s3",
        "main-cmi-ipps__s3",
        "FY19 CMIs - V36 Billed DRGs (FR 2022).txt",
        sha("s3"),
        3,
        "main-cmi-ipps__s3",
        content=(CMI_HEADER, '010001\t1.9837\t"6,752"\t"13,394.11"', "010006\t1.7000\t5000\t8500.00"),
    ),
)


def with_record(item: Stored, record: tuple[tuple[str, str], ...]) -> Stored:
    """Return a wide-table object with one more row."""
    return replace(item, records=(*item.records, record), rows=item.rows + 1)


def cc(**fields: str) -> tuple[tuple[str, str], ...]:
    """Return one Care Compare row from its bronze columns."""
    return tuple(fields.items())


DATES_2022 = {"start_date": "01/01/2022", "end_date": "12/31/2022"}
OLD_DATES_2022 = {"measure_start_date": "01/01/2022", "measure_end_date": "12/31/2022"}
# Care Compare measure windows and a second Hospital General Information file on the same release date [318] to [329].
GROUP_B = (
    # Timely and effective care: a later release revises a value; an older layout (provider_id, measure_start_date); a date
    # that does not parse; two files on one release date that disagree [319] [320] [322].
    Stored(
        "cms_cc_timely_and_effective_care_hospital",
        "te1",
        "2024-01-31",
        "Timely_and_Effective_Care-Hospital.csv",
        sha("w1"),
        3,
        records=(
            cc(facility_id="010001", measure_id="OP_18b", condition="Emergency Department", score="150", sample="300", **DATES_2022),
            cc(facility_id="010001", measure_id="SEP_1", score="55", sample="80", **DATES_2022),
            cc(facility_id="010002", measure_id="EDV", score="high", **DATES_2022),
        ),
    ),
    Stored(
        "cms_cc_timely_and_effective_care_hospital",
        "te2",
        "2024-04-30",
        "timely_effective_older_layout.csv",
        sha("w2"),
        3,
        records=(
            cc(provider_id="010001", measure_id="OP_18b", score="152", sample="310", **OLD_DATES_2022),
            cc(provider_id="010003", measure_id="SEP_1", score="40", measure_start_date="13/45/2022", measure_end_date="12/31/2022"),
            cc(provider_id="010001", measure_id="SEP_1", score="56", **OLD_DATES_2022),
        ),
    ),
    Stored(
        "cms_cc_timely_and_effective_care_hospital",
        "te3",
        "2024-04-30",
        "Timely_and_Effective_Care-Hospital_supplement.csv",
        sha("w3"),
        1,
        records=(cc(facility_id="010001", measure_id="SEP_1", score="57", **DATES_2022),),
    ),
    # Maternal health: start_date and end_date only [319].
    Stored(
        "cms_cc_maternal_health_hospital",
        "mt1",
        "2025-10-01",
        "Maternal_Health-Hospital.csv",
        sha("w4"),
        2,
        records=(
            cc(facility_id="010001", measure_id="SM_7", score="Yes", start_date="01/01/2023", end_date="12/31/2023"),
            cc(facility_id="010001", measure_id="PC_02", score="30", sample="100", start_date="01/01/2023", end_date="12/31/2023"),
        ),
    ),
    # HCAHPS: each value column stays apart; the facility-level counts repeat on every row [321] [326].
    Stored(
        "cms_cc_hcahps_hospital",
        "hc1",
        "2024-01-31",
        "HCAHPS-Hospital.csv",
        sha("w5"),
        2,
        records=(
            cc(
                facility_id="010001",
                hcahps_measure_id="H_STAR_RATING",
                patient_survey_star_rating="4",
                hcahps_answer_percent="Not Applicable",
                number_of_completed_surveys="507",
                survey_response_rate_percent="21",
                **DATES_2022,
            ),
            cc(
                facility_id="010001",
                hcahps_measure_id="H_COMP_1_A_P",
                hcahps_answer_percent="80",
                number_of_completed_surveys="507",
                survey_response_rate_percent="21",
                **DATES_2022,
            ),
        ),
    ),
    # A second Hospital General Information file on the same release date, in the older provider_id layout [328].
    Stored(
        "cms_cc_hospital_general_information",
        "g2",
        "2024-01-31",
        "Hospital General Information.csv",
        sha("u5"),
        1,
        records=(
            (
                ("provider_id", "010001"),
                ("state", "AL"),
                ("hospital_type", "Acute Care Hospitals"),
                ("emergency_services", "Yes"),
                ("hospital_overall_rating", "3"),
            ),
        ),
    ),
)


# Cost reports [331] to [339]: a full year with every ratio's inputs, scientific notation, a negative amount and a ZIP with a
# trailing hyphen; a short report with zero denominators and a ZIP+4; a second report of one CCN in one fiscal year, with a
# 9-digit ZIP and NA; a full year that starts in November, counted in the next fiscal year.
COSTS_2023 = (
    cc(
        rpt_rec_num="100001",
        provider_ccn="010001",
        fiscal_year_begin_date="10/01/2022",
        fiscal_year_end_date="09/30/2023",
        zip_code="35233-",
        rural_versus_urban="U",
        type_of_control="2",
        provider_type="1",
        number_of_beds="250",
        total_bed_days_available="91250",
        total_days_v_xviii_xix_unknown="73000",
        total_days_title_xviii="36500",
        total_days_title_xix="7300",
        total_discharges_v_xviii_xix_unknown="14600",
        fte_employees_on_payroll="1500.5",
        contract_labor_direct_patient_care="4380000",
        net_patient_revenue="500000000",
        net_income_from_service_to_patients="62000000",
        less_total_operating_expense="438000000",
        total_salaries_from_worksheet_a="219000000",
        total_current_assets="200000000",
        total_current_liabilities="100000000",
        total_liabilities="300000000",
        total_assets="600000000",
        cash_on_hand_and_in_banks="36000000",
        cost_of_charity_care="5000000",
        cost_of_uncompensated_care="8000000",
        total_costs="400000000",
        net_revenue_from_medicaid="50000000",
        cost_to_charge_ratio="2.5E-1",
        total_bad_debt_expense="-10",
    ),
    cc(
        rpt_rec_num="100002",
        provider_ccn="010002",
        fiscal_year_begin_date="01/01/2023",
        fiscal_year_end_date="06/30/2023",
        zip_code="35233-1234",
        rural_versus_urban="R",
        number_of_beds="0",
        total_bed_days_available="0",
        fte_employees_on_payroll="100",
        total_current_assets="10",
        total_current_liabilities="0",
    ),
    cc(
        rpt_rec_num="100003",
        provider_ccn="010001",
        fiscal_year_begin_date="07/01/2023",
        fiscal_year_end_date="09/30/2023",
        zip_code="352331234",
        rural_versus_urban="NA",
    ),
)
COSTS_2022 = (
    cc(rpt_rec_num="090001", provider_ccn="010001", fiscal_year_begin_date="10/01/2021", fiscal_year_end_date="09/30/2022"),
    cc(rpt_rec_num="090002", provider_ccn="010005", fiscal_year_begin_date="11/15/2021", fiscal_year_end_date="11/14/2022"),
)


# IPPS impact files [342] to [353]: a title row over a long-name header with every mapped field, SAS dots, blanks, leading
# spaces and a footer row; a comma-separated file with a comma inside a quoted name and the pre-FY 2011 wage index; a
# workbook with a final-rule sheet, a correction-notice sheet with revised columns and lost leading zeros, and a description
# sheet; a headerless file and a policy alternative, both excluded by family.
IMPACT_2026_HEADER = (
    "Provider Number\tName\tGeographic Labor Market Area\tURGEO\tFY 2026 Wage Index\tBeds\tAverage Daily Census\t"
    "Resident to Bed Ratio\tRDAY\tDSHPCT\tMedicare Percentage\tMedicaid Percentage\tTCHOP\tTCHCP\tBILLS"
)
IMPACT_2026 = (
    "FY 2026 IPPS Impact File -  Final Rule (August 2025)\t\t\t",
    IMPACT_2026_HEADER,
    "010001\tSoutheast Health Medical Center\t20020\tOURBAN\t0.9049\t400\t300\t0.0655\t0.05496\t0.2752\t0.187\t0.013\t0.0298\t0.05731\t3952",
    "010005\tMarshall Medical Center\t01\tRURAL\t0.8\t100\t.\t\t0.1\t.\t \t0.02\t0\t0\t  1200",
    "Total\t\t\t\t\t\t\t",
)
IMPACT_2009 = (
    "Provider Number,Name,Geographic Labor Market Area,URGEO,Post Reclass Wage Index,BEDS,Average Daily Census,"
    "Resident to Bed Ratio,DSHPCT,MCR_PCT,TCHOP,TCHCP,BILLS",
    '010001,"ALPHA, INC",20020,OURBAN,0.8401,370,242,0.28515,0.1274,27.3%,0.05944,0.028,10216',
)
IMPACT_2015_WORKBOOK = (
    "impact_puf15:Provider Number|Name|FY 2015 Wage Index|BILLS|Beds",
    "impact_puf15:10001|ALPHA|0.75|500|50",
    "impact_puf15-Sept CN:Provider Number|Name|Revised under correction notice: FY 2015 Wage Index|Revised under correction notice: BILLS|Beds",
    "impact_puf15-Sept CN:10001|ALPHA|0.76|510|50",
    "Variable Description:Variable|Description",
    "Variable Description:BEDS|Number of beds",
)
GROUP_B3 = (
    # A CCN repeated within one release is held [351].
    Stored(
        "cms_ipps_text_lines",
        "p06",
        "CMS_IPPS__q",
        "FY 2025 IPPS Final Rule Impact File.txt",
        sha("im6"),
        4,
        content=("Provider Number\tBeds", "010009\t10", "010009\t10", "010010\t20"),
    ),
    Stored("cms_ipps_text_lines", "p01", "CMS_IPPS__q", "FY 2026 IPPS Final Rule Impact File.txt", sha("im1"), 5, content=IMPACT_2026),
    Stored("cms_ipps_text_lines", "p02", "CMS_IPPS__q", "imppuf09_080929.csv", sha("im2"), 2, content=IMPACT_2009),
    Stored("cms_ipps_sheet_rows", "p03", "CMS_IPPS__q", "FY 2015 IPPS Final Rule Impact PUF-CN.xlsx", sha("im3"), 6, content=IMPACT_2015_WORKBOOK),
    Stored("cms_ipps_text_lines", "p04", "CMS_IPPS__q", "IMPFIL00.txt", sha("im4"), 1, content=("010001 SOUTHEAST 198 400 8041.25",)),
    Stored(
        "cms_ipps_text_lines",
        "p05",
        "CMS_IPPS__q",
        "FY 2023 IPPS Proposed Rule Impact File - Alternative Considered.txt",
        sha("im5"),
        2,
        content=("Provider Number\tName\tFY 2023 Wage Index", "010001\tAlpha\t0.8"),
    ),
)


# Medicare inpatient [355] to [363]: a provider summary with every measure input, a suppressed race count and blank
# chronic-condition shares; a provider with suppressed totals; a data year in a lower-case file name; DRG cells for the
# sepsis share.
MUP_2023_FULL = cc(
    rndrng_prvdr_ccn="010001",
    rndrng_prvdr_state_abrvtn="AL",
    tot_benes="1000",
    tot_dschrgs="1500",
    tot_days="7500",
    tot_cvrd_days="7400",
    bene_avg_age="74.5",
    bene_avg_risk_scre="1.8",
    bene_age_lt_65_cnt="100",
    bene_age_65_74_cnt="400",
    bene_age_75_84_cnt="300",
    bene_age_gt_84_cnt="200",
    bene_feml_cnt="550",
    bene_male_cnt="450",
    bene_race_wht_cnt="700",
    bene_race_black_cnt="200",
    bene_race_api_cnt="30",
    bene_race_hspnc_cnt="50",
    bene_race_othr_cnt="20",
    bene_dual_cnt="250",
    bene_ndual_cnt="750",
    bene_cc_ph_diabetes_v2_pct="0.35",
    bene_cc_ph_ckd_v2_pct="0.4",
    bene_cc_bh_depress_v1_pct="0.3",
)
MUP_2023_SUPPRESSED = cc(rndrng_prvdr_ccn="010005", rndrng_prvdr_state_abrvtn="AL", tot_dschrgs="12")
GROUP_B4 = (
    Stored(
        "cms_medicare_inpatient_by_provider",
        "mp1",
        "CMS_MEDICARE_PROVIDER__a",
        "MUP_INP_RY25_P04_V10_DY23_Prv.CSV",
        sha("mp1"),
        2,
        records=(MUP_2023_FULL, MUP_2023_SUPPRESSED),
    ),
    Stored(
        "cms_medicare_inpatient_by_provider",
        "mp2",
        "CMS_MEDICARE_PROVIDER__a",
        "mup_inp_ry26_p04_v10_dy24_prv.csv",
        sha("mp2"),
        1,
        records=(cc(rndrng_prvdr_ccn="010001", tot_benes="900", tot_dschrgs="1350"),),
    ),
    # The DRG file of a data year can come from an earlier release than its provider file, as for data years 2013 to
    # 2021 (provider release 2024, DRG release 2023).
    Stored(
        "cms_medicare_inpatient_by_drg",
        "md1",
        "CMS_MUP_DRG__a",
        "MUP_INP_RY24_P03_V10_DY23_PrvSvc.CSV",
        sha("md1"),
        4,
        records=(
            cc(rndrng_prvdr_ccn="010001", drg_cd="871", tot_dschrgs="60"),
            cc(rndrng_prvdr_ccn="010001", drg_cd="872", tot_dschrgs="30"),
            cc(rndrng_prvdr_ccn="010001", drg_cd="470", tot_dschrgs="100"),
            cc(rndrng_prvdr_ccn="010005", drg_cd="871", tot_dschrgs="12"),
        ),
    ),
)


# AL2 [564] to [575]: windows that end before the 2019 and 2021 HAI windows start (two for OP_18b, so the later one wins),
# a C141 spelling in another case, two files on one release date that disagree on 010005's 2020 OP_18b window (held in
# staging), an overall rating released before the 2021 window and two files on one later date that disagree for 010005.
DATES_2018 = {"start_date": "01/01/2018", "end_date": "12/31/2018"}
DATES_2020 = {"start_date": "01/01/2020", "end_date": "12/31/2020"}
GROUP_AL2 = (
    Stored(
        "cms_cc_timely_and_effective_care_hospital",
        "te_al2a",
        "2021-03-31",
        "Timely_and_Effective_Care-Hospital_al2.csv",
        sha("z6"),
        4,
        records=(
            cc(facility_id="010001", measure_id="OP_18b", score="130", **DATES_2018),
            cc(facility_id="010001", measure_id="OP_18b", score="145", **DATES_2020),
            cc(facility_id="010001", measure_id="EDV", score="Low", **DATES_2020),
            cc(facility_id="010005", measure_id="OP_18b", score="120", **DATES_2020),
        ),
    ),
    Stored(
        "cms_cc_timely_and_effective_care_hospital",
        "te_al2b",
        "2021-03-31",
        "Timely_and_Effective_Care-Hospital_al2_supplement.csv",
        sha("z7"),
        1,
        records=(cc(facility_id="010005", measure_id="OP_18b", score="125", **DATES_2020),),
    ),
    Stored(
        "cms_cc_hospital_general_information",
        "g_al2a",
        "2020-07-01",
        "Hospital_General_Information_2020-07.csv",
        sha("z8"),
        1,
        records=((("facility_id", "010001"), ("state", "AL"), ("hospital_type", "Acute Care Hospitals"), ("hospital_overall_rating", "2")),),
    ),
    Stored(
        "cms_cc_hospital_general_information",
        "g_al2b",
        "2020-10-01",
        "Hospital_General_Information_2020-10.csv",
        sha("z9"),
        1,
        records=((("facility_id", "010005"), ("state", "AL"), ("hospital_type", "Acute Care Hospitals"), ("hospital_overall_rating", "3")),),
    ),
    Stored(
        "cms_cc_hospital_general_information",
        "g_al2c",
        "2020-10-01",
        "Hospital_General_Information_2020-10_supplement.csv",
        sha("r6"),
        1,
        records=((("facility_id", "010005"), ("state", "AL"), ("hospital_type", "Acute Care Hospitals"), ("hospital_overall_rating", "5")),),
    ),
)


def owner(enrollment: str, owner_id: str, role: str, **fields: str) -> tuple[tuple[str, str], ...]:
    """Return one organisation owner row: the hospital enrollment, the owner, the role and any other bronze columns."""
    return (("enrollment_id", enrollment), ("associate_id_owner", owner_id), ("type_owner", "O"), ("role_code_owner", role), *fields.items())


CHOW_EVENT = cc(
    enrollment_id_buyer="O20000000004",
    ccn_buyer="10002900",
    enrollment_id_seller="O20000000005",
    ccn_seller="100029",
    chow_type_code="CH",
    chow_type_text="CHANGE OF OWNERSHIP",
    effective_date="03/01/2021",
)
# AL3b [587] to [594]: HHS weeks of 2020 for 010001 (a ratio of sums and a sum; a December 2019 week outside the year) and
# a 2020 week where two HHS hospitals share 010005's CCN (skipped); an owner release before the 2021 window where a direct
# owner reports private equity and a managing organisation reports a REIT (a role that does not count).
GROUP_AL3B = (
    Stored(
        "hhs_capacity_csv",
        "hh_al3b",
        "HHS_CAPACITY__fixture_al3b",
        "rows.csv",
        sha("r7"),
        5,
        records=(
            cc(
                hospital_pk="010001",
                collection_week="2019/12/29",
                ccn="010001",
                all_adult_hospital_inpatient_bed_occupied_7_day_avg="90",
                all_adult_hospital_inpatient_beds_7_day_avg="100",
                previous_day_admission_influenza_confirmed_7_day_sum="9",
            ),
            cc(
                hospital_pk="010001",
                collection_week="2020/01/05",
                ccn="010001",
                all_adult_hospital_inpatient_bed_occupied_7_day_avg="50",
                all_adult_hospital_inpatient_beds_7_day_avg="100",
                previous_day_admission_influenza_confirmed_7_day_sum="3",
            ),
            cc(
                hospital_pk="010001",
                collection_week="2020/01/12",
                ccn="010001",
                all_adult_hospital_inpatient_bed_occupied_7_day_avg="60",
                all_adult_hospital_inpatient_beds_7_day_avg="100",
                previous_day_admission_influenza_confirmed_7_day_sum="4",
            ),
            cc(
                hospital_pk="010005",
                collection_week="2020/01/05",
                ccn="010005",
                all_adult_hospital_inpatient_bed_occupied_7_day_avg="70",
                all_adult_hospital_inpatient_beds_7_day_avg="100",
            ),
            cc(
                hospital_pk="010005B",
                collection_week="2020/01/05",
                ccn="010005",
                all_adult_hospital_inpatient_bed_occupied_7_day_avg="20",
                all_adult_hospital_inpatient_beds_7_day_avg="40",
            ),
        ),
    ),
    Stored(
        "cms_hospital_owners",
        "ow0",
        "CMS_OWNERS_ORG__fixture_v0",
        "organisation_owners.csv",
        sha("r8"),
        2,
        records=(
            owner("O20000000001", "1111111111", "34", percentage_ownership="60", private_equity_company_owner="Y", reit_owner="N"),
            owner("O20000000001", "2222222222", "43", private_equity_company_owner="N", reit_owner="Y"),
        ),
    ),
)


# B5a ownership. Owners in the layout before April 2025, without the private-equity and REIT columns, and after it, with a
# blank flag, a one-digit date and decimal shares [366] [368] [369]; an enrollment whose CCN lost its leading zero and a
# unit CCN [370]; a change of ownership repeated in a later cumulative release, with a buyer value that is not a CCN
# [370] [372].
GROUP_B5A = (
    Stored(
        "cms_hospital_owners",
        "ow1",
        "CMS_OWNERS_ORG__fixture_v1",
        "organisation_owners.csv",
        sha("o2"),
        2,
        records=(
            owner("O20000000002", "9876543210", "34", association_date_owner="3/7/2020", percentage_ownership="100", for_profit_owner="Y", llc_owner="Y"),
            owner("O20000000002", "5555555555", "43", association_date_owner="11/30/2019", for_profit_owner="Y"),
        ),
    ),
    Stored(
        "cms_hospital_owners",
        "ow2",
        "CMS_OWNERS_ORG__fixture_v2",
        "organisation_owners.csv",
        sha("o3"),
        3,
        records=(
            owner(
                "O20000000002",
                "9876543210",
                "34",
                association_date_owner="3/7/2020",
                percentage_ownership="62.5",
                for_profit_owner="Y",
                private_equity_company_owner="Y",
                reit_owner="N",
                chain_home_office_owner="N",
                owned_by_another_org_or_ind_owner="Y",
            ),
            owner(
                "O20000000002",
                "1111111111",
                "35",
                association_date_owner="04/15/2025",
                percentage_ownership="37.5",
                private_equity_company_owner="N",
                reit_owner="N",
            ),
            owner("O20000000002", "5555555555", "43", private_equity_company_owner=""),
        ),
    ),
    Stored(
        "cms_hospital_enrollments",
        "en1",
        "ENROLL__fixture_b",
        "Hospital_Enrollments_2022.11.01.csv",
        sha("v1"),
        7,
        records=(
            cc(
                enrollment_id="O20000000002",
                ccn="13025",
                npi="1234567893",
                provider_type_code="00-09",
                proprietary_nonprofit="P",
                incorporation_date="1/5/1990",
                subgroup_acute_care="Y",
                reh_conversion_flag="N",
            ),
            cc(enrollment_id="O20000000003", ccn="01T001", subgroup_acute_care="N"),
            # [619] to [621] A location suffix, a lost leading zero and a unit letter map to the parent POS lists in the
            # row's state; the same suffix in another state and an unknown shape stay without a CCN.
            cc(enrollment_id="O20000000010", ccn="01000101", state="AL"),
            cc(enrollment_id="O20000000011", ccn="1000101", state="AL"),
            cc(enrollment_id="O20000000012", ccn="01S001A", state="AL"),
            cc(enrollment_id="O20000000013", ccn="01000101", state="GA"),
            cc(enrollment_id="O20000000014", ccn="78A005BP", state="IL"),
        ),
    ),
    Stored("cms_change_of_ownership", "xw1", "CMS_CHOW__fixture_q", "Hospital_CHOW_2022Q1.csv", sha("y1"), 1, records=(CHOW_EVENT,)),
    Stored(
        "cms_change_of_ownership",
        "xw2",
        "CMS_CHOW__fixture_r",
        "Hospital_CHOW_2022.09.30.csv",
        sha("y2"),
        2,
        records=(
            CHOW_EVENT,
            cc(
                enrollment_id_buyer="O20000000006",
                ccn_buyer="13025",
                enrollment_id_seller="O20000000007",
                ccn_seller="01T001",
                chow_type_code="AM",
                chow_type_text="ACQUISITION/MERGER",
                effective_date="7/1/2022",
            ),
        ),
    ),
)


# B5b HHS and ONC. Two weeks of one hospital, with a suppressed count, a corrected week and a coverage count, and a hospital
# without a CCN [375] to [379]; a CHPL linkage row with a telephone number and a blank criterion [380] to [382]; one
# older attestation [383].
HHS_WEEK = cc(
    hospital_pk="010001",
    collection_week="2021/01/03",
    ccn="010001",
    state="AL",
    hospital_subtype="Short Term",
    is_metro_micro="true",
    is_corrected="false",
    total_beds_7_day_avg="250.5",
    inpatient_beds_used_7_day_avg="180",
    inpatient_beds_used_covid_7_day_avg="-999999",
    total_beds_7_day_coverage="7",
    previous_day_admission_adult_covid_confirmed_50_59_7_day_sum="-999999",
    staffed_pediatric_icu_bed_occupancy_7_day_avg="-9",
)
GROUP_B5B = (
    Stored(
        "hhs_capacity_csv",
        "hh1",
        "HHS_CAPACITY__fixture",
        "rows.csv",
        sha("z1"),
        4,
        records=(
            HHS_WEEK,
            cc(hospital_pk="010001", collection_week="2021/01/10", ccn="010001", state="AL", is_corrected="true", total_beds_7_day_avg="251"),
            cc(hospital_pk="3f" * 32, collection_week="2021/01/03", state="AL", total_beds_7_day_avg="40"),
            # [681] [682] The facility in the reviewed matches seed gets its CCN; the one above stays without.
            cc(
                hospital_pk="ee04edd185865c38c839812cb2eb5ae5d3f8922e3b629ee98c7d9424a37826c4",
                collection_week="2021/01/03",
                state="LA",
                total_beds_7_day_avg="30",
            ),
        ),
    ),
    Stored(
        "onc_pi_chpl_linkage_csv",
        "oc1",
        "ONC_PI__fixture",
        "hospital-promoting-interoperability-chpl-linkage.csv",
        sha("z2"),
        2,
        records=(
            cc(
                facility_id="010001",
                telephone_number="telephone-placeholder",
                meets_criteria_for_promoting_interoperability_of_ehrs="Y",
                start_date="1/1/2023",
                end_date="12/31/2023",
                cehrt_id="0015EFIXTURE01",
                chpl_id="15.04.04.1234.Epic.AM.01.1.220101",
                product_database_id="11111",
                developer_name="Fixture Developer A",
                product_name="Fixture EHR",
                year="2023",
            ),
            cc(
                facility_id="010005",
                start_date="07/01/2024",
                end_date="09/30/2024",
                chpl_id="15.04.04.2345.Cern.01.01.1.220202",
                product_database_id="22222",
                developer_name="Fixture Developer B",
                year="2024",
            ),
        ),
    ),
    Stored(
        "onc_pi_attestations_csv",
        "oa1",
        "ONC_PI__fixture_att",
        "hospital_attestations.csv",
        sha("z3"),
        1,
        records=(
            cc(
                npi="1234567893",
                ccn="010001",
                program_type="Medicare/Medicaid",
                program_year="2014",
                payment_year="3",
                attestation_month="7",
                attestation_year="2014",
                vendor_name="Fixture Developer A",
            ),
        ),
    ),
)


def hud(zip_code: str, geoid: str, res: str, tot: str, state: str = "") -> tuple[tuple[str, str], ...]:
    """Return one HUD ZIP-to-county row with its residential and total ratios; business and other ratios equal the total."""
    return (("zip", zip_code), ("geoid", geoid), ("res_ratio", res), ("bus_ratio", tot), ("oth_ratio", tot), ("tot_ratio", tot), ("state", state))


def adjacent(county: str, code: str, neighbor: str, neighbor_code: str, length: str = "") -> tuple[tuple[str, str], ...]:
    """Return one county adjacency row; an island has an empty neighbor."""
    return (("county_name", county), ("county_geoid", code), ("neighbor_name", neighbor), ("neighbor_geoid", neighbor_code), ("length", length))


def rucc(fips: str, state: str, county: str, attribute: str, value: str) -> tuple[tuple[str, str], ...]:
    """Return one row of the long 2023 RUCC file."""
    return (("fips", fips), ("state", state), ("county_name", county), ("attribute", attribute), ("value", value))


def hsa(ccn: str, zip_code: str, cases: str, days: str, charges: str) -> tuple[tuple[str, str], ...]:
    """Return one hospital service area row."""
    fields = {"medicare_prov_num": ccn, "zip_cd_of_residence": zip_code, "total_cases": cases, "total_days_of_care": days, "total_charges": charges}
    return tuple(fields.items())


# HUD quarters and service-area years as their receipts and job plans record them, keyed by release [403] [416].
GEOGRAPHY_QUARTERS = {"HUD_API__q1": ("2021Q1", "2021-01-01", "2021-03-31"), "HUD_XLSX__q2": ("2020Q4", "2020-10-01", "2020-12-31")}
GEOGRAPHY_COVERAGE = {"HSA__y15": ("2015-01-01", "2015-12-31"), "HSA__y16": ("2016-01-01", "2016-12-31")}
RUCC_2013_HEADER = "FIPS|State|County_Name|Population_2010|RUCC_2013|Description"
RUCA_2010_HEADER = (
    "State-County FIPS Code|Select State|Select County|State-County-Tract FIPS Code (lookup by address at http://www.ffiec.gov/Geocode/)"
    "|Primary RUCA Code 2010|Secondary RUCA Code, 2010 (see errata)|Tract Population, 2010|Land Area (square miles), 2010"
    "|Population Density (per square mile), 2010"
)
# C1 geography [400] to [419]: HUD with a ZIP without residential addresses, Connecticut, a territory, a scientific-notation
# ratio and two non-county rows; adjacency with islands, self-links, a zero length and a nameless 2010 lead line; RUCC and
# RUCA vintages with blank and 99 codes; service areas with suppressed rows, cells and ZIPs.
GROUP_C1 = (
    Stored(
        "hud_zip_county",
        "hz1",
        "HUD_API__q1",
        "crosswalk.csv",
        sha("hz1"),
        9,
        records=(
            hud("00501", "36103", "0", "1", "NY"),
            hud("01001", "25013", "1", "1", "MA"),
            hud("06001", "09110", "1", "1", "CT"),
            hud("96799", "60", "0", "1", "AS"),
            hud("53001", "99999", "0", "0.0005", "WI"),
            hud("53001", "55117", "1", "0.9995", "WI"),
            hud("20001", "11001", "0.75", "0.75", "DC"),
            hud("20001", "24031", "2.5E-1", "0.25", "DC"),
            hud("00601", "72001", "1", "1", "PR"),
        ),
    ),
    Stored("hud_zip_county", "hz2", "HUD_XLSX__q2", "crosswalk.csv", sha("hz2"), 2, records=(hud("01001", "25013", "1", "1"), hud("35004", "01073", "1", "1"))),
    Stored(
        "hud_zip_county_sheet_rows",
        "hw2",
        "HUD_XLSX__q2",
        "ZIP-COUNTY_122020.xlsx",
        sha("hw2"),
        3,
        content=("Sheet1:zip|geoid|res_ratio|bus_ratio|oth_ratio|tot_ratio", "Sheet1:01001|25013|1|1|1|1", "Sheet1:35004|01073|1|1|1|1"),
    ),
    Stored(
        "county_adjacency",
        "aj1",
        "ADJ__a25",
        "county_adjacency2025.txt",
        sha("aj1"),
        5,
        records=(
            adjacent("Autauga County, AL", "01001", "Chilton County, AL", "01021", "12345.6"),
            adjacent("Chilton County, AL", "01021", "Autauga County, AL", "01001", "12345.6"),
            adjacent("Kauai County, HI", "15007", "", ""),
            adjacent("Western Connecticut Planning Region, CT", "09190", "Capitol Planning Region, CT", "09110", "0"),
            adjacent("Capitol Planning Region, CT", "09110", "Western Connecticut Planning Region, CT", "09190", "0"),
        ),
    ),
    Stored(
        "county_adjacency",
        "aj2",
        "ADJ__a24",
        "county_adjacency2024.txt",
        sha("aj2"),
        4,
        records=(
            adjacent("Autauga County, AL", "01001", "Autauga County, AL", "01001"),
            adjacent("Autauga County, AL", "01001", "Chilton County, AL", "01021"),
            adjacent("Chilton County, AL", "01021", "Autauga County, AL", "01001"),
            adjacent("Chilton County, AL", "01021", "Chilton County, AL", "01021"),
        ),
    ),
    Stored(
        "county_adjacency_2010_text_lines",
        "at0",
        "ADJ__a10",
        "county_adjacency2010.txt",
        sha("at0"),
        7,
        content=(
            '"Autauga County, AL"\t01001\t"Autauga County, AL"\t01001',
            '\t\t"Chilton County, AL"\t01021',
            '"Chilton County, AL"\t01021\t"Autauga County, AL"\t01001',
            '\t\t"Chilton County, AL"\t01021',
            '\t27165\t"Blue Earth County, MN"\t27013',
            '\t\t"Watonwan County, MN"\t27165',
            '"Blue Earth County, MN"\t27013\t"Watonwan County, MN"\t27165',
        ),
    ),
    Stored(
        "rucc",
        "rc1",
        "RUCC__c23",
        "2023-rural-urban-continuum-codes.csv",
        sha("rc1"),
        8,
        records=(
            rucc("01001", "AL", "Autauga County", "Population_2020", "58805"),
            rucc("01001", "AL", "Autauga County", "RUCC_2023", "2"),
            rucc("01001", "AL", "Autauga County", "Description", "Metro - Counties in metro areas of 250,000 to 1 million population"),
            rucc("09120", "CT", "Greater Bridgeport Planning Region", "Population_2020", "902412"),
            rucc("09120", "CT", "Greater Bridgeport Planning Region", "RUCC_2023", "1"),
            rucc("09120", "CT", "Greater Bridgeport Planning Region", "Description", "Metro - Counties in metro areas of 1 million population or more"),
            rucc("09001", "CT", "Fairfield County", "Population_2020", "957419"),
            rucc("09001", "CT", "Fairfield County", "Description", "Not Applicable"),
        ),
    ),
    Stored(
        "rucc_sheet_rows",
        "rs1",
        "RUCC__s13",
        "2013-rural-urban-continuum-codes.xls",
        sha("rs1"),
        4,
        content=(
            f"Rural-urban Continuum Code 2013:{RUCC_2013_HEADER}",
            "Rural-urban Continuum Code 2013:01001|AL|Autauga County|54571.0|2.0|Metro - Counties in metro areas of 250,000 to 1 million population",
            "Rural-urban Continuum Code 2013:02105|AK|Hoonah-Angoon Census Area|2150.0||",
            "Documentation:Rural-urban continuum codes, 2013",
        ),
    ),
    Stored(
        "rucc_sheet_rows",
        "rs2",
        "RUCC__s23",
        "2023-rural-urban-continuum-codes.xlsx",
        sha("rs2"),
        2,
        content=(
            "Rural-urban Continuum Code 2023:FIPS|State|County_Name|Population_2020|RUCC_2023|Description",
            "Rural-urban Continuum Code 2023:01001|AL|Autauga County|58805|2|Metro - Counties in metro areas of 250,000 to 1 million population",
        ),
    ),
    Stored(
        "ruca_tracts_2020",
        "rt1",
        "RUCA__t20",
        "2020-rural-urban-commuting-area-codes-census-tracts.csv",
        sha("rt1"),
        3,
        records=(
            cc(tractfips20="01001020100", countyfips20="01001", countyfips23="01001", statefips20="01", primaryruca="1", secondaryruca="1"),
            cc(tractfips20="09001010101", countyfips20="09001", countyfips23="09190", statefips20="09", primaryruca="2", secondaryruca="2.1"),
            cc(tractfips20="01001990000", countyfips20="01001", countyfips23="01001", statefips20="01", primaryruca="99", secondaryruca="99"),
        ),
    ),
    Stored(
        "ruca_zip_2020",
        "rz1",
        "RUCA__z20",
        "2020-rural-urban-commuting-area-codes-zip-codes.csv",
        sha("rz1"),
        2,
        records=(
            cc(zipcode="00501", state="NY", zipcodetype="Post Office or large volume customer", primaryruca="1", secondaryruca="1"),
            cc(zipcode="99950", state="AK", zipcodetype="ZIP Code Area", primaryruca="10", secondaryruca="10.3"),
        ),
    ),
    Stored(
        "ruca_zip_2010",
        "ry1",
        "RUCA__z10",
        "2010-rural-urban-commuting-area-codes-zip-code-file.csv",
        sha("ry1"),
        1,
        records=(cc(zip_code="''00501''", state="NY", zip_type="Post Office or large volume customer", ruca1="1", ruca2="1.1"),),
    ),
    Stored(
        "ruca_sheet_rows",
        "rsh",
        "RUCA__s10",
        "2010-rural-urban-commuting-area-codes-revised-732019.xlsx",
        sha("rsh"),
        5,
        content=(
            "Data:Errata: On July 3, 2019, the RUCA codes were revised.",
            f"Data:{RUCA_2010_HEADER}",
            "Data:01001|AL|Autauga County|01001020100|1|1|1912|3.78764071493768|504.7997273",
            "Data:72153|PR|Yauco Municipio|72153750602|4|4.1|3141|6.76703328355189|464.1620439",
            "RUCA code description:1 Metropolitan area core",
        ),
    ),
    Stored(
        "cms_hsa_csv",
        "hs1",
        "HSA__y15",
        "HSAF_2015_SUPPRESS.csv",
        sha("hs1"),
        4,
        records=(
            hsa("010001", "32420", "23", "130", "915149"),
            hsa("010001", "*", "", "", ""),
            hsa("010001", "*", "", "", ""),
            hsa("010001", "     ", "4", "8", "90"),
        ),
    ),
    Stored(
        "cms_hsa_csv",
        "hs2",
        "HSA__y16",
        "Hospital_Service_Area_2016.csv",
        sha("hs2"),
        3,
        records=(
            hsa("010001", "32420", "*", "*         ", "*         "),
            hsa("01T001", "32421", "12", "60", "50000"),
            hsa("10001", "32422", "15", "70", "1000"),
        ),
    ),
)


def acs(geo: str, **values: str) -> tuple[tuple[str, str], ...]:
    """Return one ACS row: its geography and the published cells."""
    return (("geo_id", geo), *values.items())


def svi_row(**values: str) -> tuple[tuple[str, str], ...]:
    """Return one SVI row from its published fields."""
    return tuple(values.items())


# SVI editions as the capture receipts' release labels name them, keyed by release [427].
SVI_EDITIONS = {"SVI__e22": "2022", "SVI__e00": "2000", "PLACES__e25": "2025", "PLACES__e20": "2020"}
BRIDGED = "Underlying Cause of Death, 1999-2020"
SINGLE_RACE = "Underlying Cause of Death, 2018-2024, Single Race"
# WONDER databases and periods as the capture receipts state them, keyed by release [453].
WONDER_DATABASES = {
    "WONDER__b": (BRIDGED, "2013-01-01", "2019-12-31"),
    "WONDER__s1824": (SINGLE_RACE, "2018-01-01", "2024-12-31"),
    "WONDER__s24": (SINGLE_RACE, "2024-01-01", "2024-12-31"),
}
DP04_LABELS = {
    "1.00 or less": "Percent!!OCCUPANTS PER ROOM!!Occupied housing units!!1.00 or less",
    "1.01 to 1.50": "Percent!!OCCUPANTS PER ROOM!!Occupied housing units!!1.01 to 1.50",
    "1.51 or more": "Percent!!OCCUPANTS PER ROOM!!Occupied housing units!!1.51 or more",
}
B19013_LABEL = "Estimate!!Median household income in the past 12 months (in 2017 inflation-adjusted dollars)"
B19013_MOE_LABEL = "Margin of Error!!Median household income in the past 12 months (in 2017 inflation-adjusted dollars)"
S1701_LABEL = "Estimate!!Percent below poverty level!!Population for whom poverty status is determined"


def acs_row(concept: str, table: str, vintage: str, column: str = "", label: str = "", status: str = "mapped", reason: str = "") -> dict[str, str]:
    """Return one row of the fixture's ACS variable map."""
    return {
        "concept_id": concept,
        "bronze_table": table,
        "vintage": vintage,
        "column_name": column,
        "published_label": label,
        "status": status,
        "reason": reason,
    }


# The fixture's reviewed map: the 1.01 to 1.50 concept moves from DP04_0077PE (2014) to DP04_0078PE (2023); the 1.51 or
# more concept is held in both; B19013 is labelled in the 2017 export and matched by code in both summary styles [420] to [424].
FIXTURE_ACS_MAP = (
    acs_row("DP04_0078PE", "acs_dp04", "2014", "dp04_0077pe", DP04_LABELS["1.01 to 1.50"]),
    acs_row("DP04_0078PE", "acs_dp04", "2023", "dp04_0078pe", DP04_LABELS["1.01 to 1.50"]),
    acs_row("DP04_0079PE", "acs_dp04", "2014", status="held", reason="fixture hold"),
    acs_row("DP04_0079PE", "acs_dp04", "2023", status="held", reason="fixture hold"),
    acs_row("S1701_C03_001E", "acs_s1701", "2023", "s1701_c03_001e", S1701_LABEL),
    acs_row("B19013_001E", "acs_b19013", "2017", "b19013_001e", B19013_LABEL),
    acs_row("B19013_001E", "acs_summary_b19013", "2018", "b19013_001e"),
    acs_row("B19013_001E", "acs_summary_b19013", "2023", "b19013_e001"),
    acs_row("B19013_001M", "acs_b19013", "2017", "b19013_001m", B19013_MOE_LABEL),
    acs_row("B19013_001M", "acs_summary_b19013", "2018", "b19013_001m"),
    acs_row("B19013_001M", "acs_summary_b19013", "2023", "b19013_m001"),
)
# C2 [420] to [434]: SVI 2022 with a -999 field and Connecticut, SVI 2000 with its label row; ACS exports whose codes move,
# (X), the text null, a top-coded median and suppression marks; summary files in both header styles with a state row and
# negative sentinels.
GROUP_C2 = (
    Stored(
        "svi",
        "sv22",
        "SVI__e22",
        "SVI_file_001.csv",
        sha("sv22"),
        2,
        records=(
            svi_row(
                st="01",
                state="Alabama",
                st_abbr="AL",
                stcnty="01001",
                county="Autauga County",
                fips="01001",
                e_totpop="58761",
                ep_pov150="20.2",
                ep_unemp="-999",
                rpl_themes="0.4",
            ),
            svi_row(
                st="09",
                state="Connecticut",
                st_abbr="CT",
                stcnty="09120",
                county="Greater Bridgeport",
                fips="09120",
                e_totpop="100",
                ep_pov150="10.0",
                rpl_themes="0.2",
            ),
        ),
    ),
    Stored(
        "svi",
        "sv00",
        "SVI__e00",
        "SVI_history_16dd4e5652b5572f.csv",
        sha("sv00"),
        2,
        records=(svi_row(state_fips="ST", cnty_fips="COU", g1v1r="P_POV"), svi_row(state_fips="01", cnty_fips="001", state_name="Alabama", g1v1r="15.2")),
    ),
    Stored(
        "acs_dp04",
        "a14",
        "ACS__d14",
        "ACSDP5Y2014.DP04-Data.csv",
        sha("a14"),
        1,
        records=(acs("0500000US01001", dp04_0077pe="2.9", dp04_0078pe="0.4"),),
        labels=(("GEO_ID", "Geography"), ("DP04_0077PE", DP04_LABELS["1.01 to 1.50"]), ("DP04_0078PE", DP04_LABELS["1.51 or more"])),
    ),
    Stored(
        "acs_dp04",
        "a23",
        "ACS__d23",
        "ACSDP5Y2023.DP04-Data.csv",
        sha("a23"),
        3,
        records=(acs("0500000US01001", dp04_0078pe="3.1"), acs("0500000US09110", dp04_0078pe="1.2"), acs("0500000US72001", dp04_0078pe="(X)")),
        labels=(("GEO_ID", "Geography"), ("DP04_0077PE", DP04_LABELS["1.00 or less"]), ("DP04_0078PE", DP04_LABELS["1.01 to 1.50"])),
    ),
    Stored(
        "acs_s1701",
        "s23",
        "ACS__s23",
        "ACSST5Y2023.S1701-Data.csv",
        sha("s23"),
        2,
        records=(acs("0500000US01001", s1701_c03_001e="15.2"), acs("0500000US35039", s1701_c03_001e="null")),
        labels=(("GEO_ID", "Geography"), ("S1701_C03_001E", S1701_LABEL)),
    ),
    Stored(
        "acs_b19013",
        "b17",
        "ACS__b17",
        "ACSDT5Y2017.B19013-Data.csv",
        sha("b17"),
        2,
        records=(acs("0500000US01001", b19013_001e="250,000+", b19013_001m="***"), acs("0500000US48301", b19013_001e="-", b19013_001m="**")),
        labels=(("GEO_ID", "Geography"), ("B19013_001E", B19013_LABEL), ("B19013_001M", B19013_MOE_LABEL)),
    ),
    Stored(
        "acs_summary_b19013",
        "u18",
        "ACS__u18",
        "acsdt5y2018-b19013.dat",
        sha("u18"),
        2,
        records=(acs("0500000US01001", b19013_001e="58000", b19013_001m="1200"), acs("0400000US01", b19013_001e="52000", b19013_001m="300")),
    ),
    Stored(
        "acs_summary_b19013",
        "u23",
        "ACS__u23",
        "acsdt5y2023-b19013.dat",
        sha("u23"),
        2,
        records=(acs("0500000US01001", b19013_e001="62000", b19013_m001="1500"), acs("0500000US01003", b19013_e001="-999999999", b19013_m001="-222222222")),
    ),
)


def saipe_line(state: str, county: str, values: dict[int, str], name: str) -> str:
    """Return one 264-character SAIPE line with each value right-aligned to end at its documented column [435]."""
    line = [" "] * 264
    for end, value in ((2, state), (6, county), *values.items()):
        for offset, char in enumerate(reversed(value)):
            line[end - 1 - offset] = char
    for offset, char in enumerate(name):
        line[193 + offset] = char
    return "".join(line)


# Ends of the SAIPE fields the fixture fills: all-ages poverty count, bounds and percent, and median household income.
SAIPE_ENDS = (15, 24, 33, 38, 43, 48, 139, 146, 153)


def saipe_values(*values: str) -> dict[int, str]:
    """Return the fixture's SAIPE values by the column each ends at."""
    return dict(zip(SAIPE_ENDS, values, strict=True))


SAHIE_PREAMBLE = (
    "       agecat          1      Age category",
    "                                0 - Under 65 years",
    "       racecat         1      Race category",
    "                                0 - All races",
    "       sexcat          1      Sex category",
    "                                0 - Both sexes",
    "       iprcat          1      Income category",
    "                                0 - All income levels",
)


def sahie_row(county: str, geocat: str = "50", agecat: str = "0", **values: str) -> tuple[tuple[str, str], ...]:
    """Return one SAHIE row of 2022 for state 01 unless the county code says otherwise."""
    state, county_code = county[:2], county[2:]
    fields = {"year": "2022", "statefips": state, "countyfips": county_code, "geocat": geocat, "agecat": agecat, "racecat": "0", "sexcat": "0", "iprcat": "0"}
    return tuple((fields | values).items())


def bls_row(measure: str, year: str, period: str, value: str, footnotes: str = "[{}]") -> tuple[tuple[str, str], ...]:
    """Return one BLS LAUS row of Autauga County."""
    fields = {"seriesid": f"LAUCN0100100000000{measure}", "county_fips": "01001", "measure_code": measure, "year": year, "period": period}
    return tuple((fields | {"value": value, "footnotes": footnotes}).items())


SAIPE_2023 = (
    saipe_line("00", "0", saipe_values("40763043", "40485829", "41040257", "12.5", "12.4", "12.6", "77719", "77533", "77905"), "United States"),
    saipe_line("01", "0", saipe_values("780043", "762230", "797856", "15.7", "15.3", "16.1", "62248", "61546", "62950"), "Alabama"),
    saipe_line("01", "1", saipe_values("7004", "5599", "8409", "11.7", "9.3", "14.1", "68857", "62667", "75047"), "Autauga County"),
)
# C3 [435] to [448]: SAIPE with US and state rows, the Alabama-only twin and a missing median; SAHIE with a subgroup, a
# state row and Kalawao's missing values; BLS with a month, an annual average and a footnoted missing value.
GROUP_C3 = (
    Stored("saipe_text_lines", "sp23", "SAIPE__y23", "est23all.txt", sha("sp23"), 3, content=SAIPE_2023),
    Stored("saipe_text_lines", "spal", "SAIPE__y23", "est23-al.txt", sha("spal"), 1, content=SAIPE_2023[2:]),
    Stored(
        "saipe_text_lines",
        "sp99",
        "SAIPE__y99",
        "est99all.dat",
        sha("sp99"),
        2,
        content=(
            saipe_line("01", "1", saipe_values("4991", "3871", "6110", "11.4", "8.9", "14.0", "39702", "37226", "42342"), "Autauga County"),
            saipe_line("01", "3", saipe_values("12000", "10000", "14000", "10.1", "8.5", "11.7", ".", ".", "."), "Baldwin County"),
        ),
    ),
    Stored(
        "sahie",
        "sh22",
        "SAHIE__y22",
        "sahie_2022.csv",
        sha("sh22"),
        4,
        records=(
            sahie_row("01001", nipr="45000", nui="4000", pctui="  8.9", pctui_moe="1.2"),
            sahie_row("01001", agecat="1", nipr="30000", nui="3500", pctui=" 11.7", pctui_moe="1.5"),
            sahie_row("01000", geocat="40", nipr="4000000", nui="400000", pctui="10.0", pctui_moe="0.3"),
            sahie_row("15005", nipr="   . ", nui="   . ", pctui="   . ", pctui_moe="   . "),
        ),
        preamble=SAHIE_PREAMBLE,
    ),
    Stored(
        "bls_laus",
        "bl1",
        "BLS_API__20260926T202341Z__fixture",
        "observations.csv",
        sha("bl1"),
        4,
        records=(
            bls_row("03", "2023", "M01", "3.1"),
            bls_row("03", "2023", "M13", "2.8"),
            bls_row("04", "2025", "M10", "-", '[{"code":"X","text":"Data unavailable due to the 2025 lapse in appropriations."}]'),
            bls_row("06", "2023", "M01", "26000"),
        ),
    ),
)


def places_row(year: str, state: str, location: str, measure: str, value: str, kind: str = "CrdPrv", **fields: str) -> tuple[tuple[str, str], ...]:
    """Return one PLACES row."""
    base = {"year": year, "stateabbr": state, "locationid": location, "measureid": measure, "datavaluetypeid": kind, "data_value": value}
    return tuple((base | fields).items())


def gv_row(year: str, level: str, code: str, **values: str) -> tuple[tuple[str, str], ...]:
    """Return one geographic variation row at the All age level."""
    return tuple(({"year": year, "bene_geo_lvl": level, "bene_geo_cd": code, "bene_age_lvl": "All"} | values).items())


def wonder_row(county: str, year: str, deaths: str, rate: str, population: str = "55000") -> tuple[tuple[str, str], ...]:
    """Return one WONDER county-year row; the year keeps a trailing space as the 2024 exports print it."""
    fields = {"county_code": county, "year": year + " " if year == "2024" else year, "year_code": year, "deaths": deaths, "population": population}
    return tuple((fields | {"crude_rate": rate}).items())


STATE_FIPS = (
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26",
    "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", "49", "50",
    "51", "53", "54", "55", "56",
)  # fmt: skip
# C4 [449] to [462]: PLACES with a measure covering every state, one that does not, a national row, a blank value and a
# release without county codes; geographic variation county and state rows with a suppressed value; WONDER in both
# databases with a suppressed county and the 2024 row repeated in the single-race exports.
GROUP_C4 = (
    Stored(
        "places",
        "pl25",
        "PLACES__e25",
        "rows.csv",
        sha("pl25"),
        58,
        records=(
            places_row("2023", "AL", "01001", "DIABETES", "12.1", low_confidence_limit="11.0", high_confidence_limit="13.2", locationname="Autauga"),
            places_row("2023", "AL", "01001", "DIABETES", "10.5", "AgeAdjPrv"),
            places_row("2023", "US", "59", "DIABETES", "11.0"),
            places_row("2023", "AL", "01003", "DIABETES", "", data_value_footnote_symbol="*"),
            places_row("2022", "CT", "09120", "LONELINESS", "30.2"),
            *(places_row("2022", "XX", state + "001", "OBESITY", "33.0") for state in STATE_FIPS),
            # [623] One name published with two codes.
            places_row("2023", "AL", "01005", "ARTHRITIS", "20.0", locationname="Twin"),
            places_row("2023", "AL", "01007", "ARTHRITIS", "21.0", locationname="Twin"),
        ),
    ),
    # [623] [624] The 2020 layout without county codes: a known name is typed from the other release; an unknown name and
    # the name with two codes stay untyped.
    Stored(
        "places",
        "pl20",
        "PLACES__e20",
        "rows.csv",
        sha("pl20"),
        3,
        records=(
            places_row("2018", "AL", "", "DIABETES", "11.0", locationname="Autauga"),
            places_row("2018", "AL", "", "DIABETES", "9.0", locationname="Nowhere"),
            places_row("2018", "AL", "", "ARTHRITIS", "19.0", locationname="Twin"),
        ),
    ),
    Stored(
        "cms_geographic_variation_csv",
        "gv1",
        "CMS_GV__x",
        "2014-2024_Original_Medicare_Geographic_Variation_Public_Use_File.csv",
        sha("gv1"),
        4,
        records=(
            gv_row("2023", "County", "01001", benes_total_cnt="9000", ma_prtcptn_rate="0.45", bene_dual_pct="*", pqi03_dbts_age_65_74="NA"),
            gv_row("2022", "County", "01001", ma_prtcptn_rate="0.44"),
            gv_row("2023", "State", "01", ma_prtcptn_rate="0.5"),
            gv_row("2023", "National", "", ma_prtcptn_rate="0.48"),
        ),
    ),
    # AL4a [604]: a 2020 county value for 010001's county (01073) before its 2021 window.
    Stored(
        "cms_geographic_variation_csv",
        "gv_al4",
        "CMS_GV__al4",
        "2014-2024_Original_Medicare_Geographic_Variation_Public_Use_File.csv",
        sha("q8"),
        1,
        records=(gv_row("2020", "County", "01073", ma_prtcptn_rate="0.3"),),
    ),
    Stored(
        "wonder_county_mortality",
        "wd1",
        "WONDER__b",
        "county_year.csv",
        sha("wd1"),
        2,
        records=(wonder_row("01001", "2018", "500", "909.1"), wonder_row("01003", "2018", "Suppressed", "Suppressed")),
    ),
    Stored(
        "wonder_county_mortality",
        "wd2",
        "WONDER__s1824",
        "county_year.csv",
        sha("wd2"),
        2,
        records=(wonder_row("01001", "2018", "500", "907.4", "55100"), wonder_row("01001", "2024", "520", "Unreliable")),
    ),
    Stored("wonder_county_mortality", "wd3", "WONDER__s24", "county_year.csv", sha("wd3"), 1, records=(wonder_row("01001", "2024", "520", "Unreliable"),)),
)

HRRP_2023 = {"start_date": "07/01/2020", "end_date": "06/30/2023"}


def hrrp_row(facility: str, measure: str, ratio: str, readmissions: str, **fields: str) -> tuple[tuple[str, str], ...]:
    """Return one Hospital Readmissions Reduction Program row for the 2020 to 2023 performance window."""
    base = {"facility_id": facility, "measure_name": measure, "excess_readmission_ratio": ratio, "number_of_readmissions": readmissions}
    return tuple((base | HRRP_2023 | fields).items())


# D1 [504] to [513]: visits with an exact and a renamed OP_32, a measure no control names, a token and an older-layout file
# whose OP_32 window a later release supersedes; deaths with a mapped mortality ID, PSI IDs E038 cannot map and a window
# repeated in one file; HRRP keyed by measure_name with tokens.
GROUP_D1 = (
    Stored(
        "cms_cc_unplanned_hospital_visits_hospital",
        "vi1",
        "2025-07-01",
        "Unplanned_Hospital_Visits-Hospital.csv",
        sha("vi1"),
        7,
        records=(
            cc(
                facility_id="010001",
                measure_id="OP_32",
                score="12.5",
                denominator="300",
                compared_to_national="No Different Than the National Rate",
                **DATES_2022,
            ),
            cc(facility_id="010001", measure_id="OP-32", score="13.0", **DATES_2022),
            cc(facility_id="010001", measure_id="READM_30_HF", score="20.1", **DATES_2022),
            cc(facility_id="010003", measure_id="OP_35_ED", score="Not Available", **DATES_2022),
            cc(facility_id="10003", measure_id="OP_36", score="7.0", **DATES_2022),
            cc(facility_id="01-003", measure_id="OP_32", score="5.0", **DATES_2022),
            # [629] A renamed OP-32 with no OP_32 row for its hospital and window enters as OP_32.
            cc(facility_id="010003", measure_id="OP-32", score="4.0", **DATES_2022),
        ),
    ),
    Stored(
        "cms_cc_unplanned_hospital_visits_hospital",
        "vi0",
        "2023-01-01",
        "Unplanned_Hospital_Visits-Hospital.csv",
        sha("vi0"),
        2,
        records=(
            cc(provider_id="010001", measure_id="OP_32", score="11.0", **OLD_DATES_2022),
            cc(provider_id="010001", measure_id="OP_36", score="9.9", measure_start_date="01/01/2020", measure_end_date="12/31/2020"),
        ),
    ),
    Stored(
        "cms_cc_complications_and_deaths_hospital",
        "de1",
        "2025-07-01",
        "Complications_and_Deaths-Hospital.csv",
        sha("de1"),
        5,
        records=(
            cc(facility_id="010001", measure_id="MORT_30_AMI", score="12.3", lower_estimate="10.9", higher_estimate="13.8", **DATES_2022),
            cc(facility_id="010001", measure_id="PSI_90", score="1.01", denominator="Not Applicable", **DATES_2022),
            cc(facility_id="010001", measure_id="PSI_90_SAFETY", score="0.99", **DATES_2022),
            cc(facility_id="010001", measure_id="MORT_30_HF", score="10.0", **DATES_2022),
            cc(facility_id="010001", measure_id="MORT_30_HF", score="10.4", **DATES_2022),
        ),
    ),
    Stored(
        "cms_cc_hospital_readmissions_reduction_program_hospital",
        "hr1",
        "2025-10-01",
        "FY_2025_Hospital_Readmissions_Reduction_Program_Hospital.csv",
        sha("hr1"),
        2,
        records=(
            hrrp_row("010001", "READM-30-HF-HRRP", "1.0123", "61", number_of_discharges="300", predicted_readmission_rate="20.1"),
            hrrp_row("010003", "READM-30-AMI-HRRP", "N/A", "Too Few to Report", number_of_discharges="N/A"),
        ),
    ),
)


def hac_row(facility: str, year: str, total: str, reduction: str, **fields: str) -> tuple[tuple[str, str], ...]:
    """Return one HAC Reduction Program row."""
    return tuple(({"facility_id": facility, "fiscal_year": year, "total_hac_score": total, "payment_reduction": reduction} | fields).items())


def tps_row(score: str, **fields: str) -> tuple[tuple[str, str], ...]:
    """Return one Hospital VBP Total Performance Score row."""
    return tuple(({"total_performance_score": score} | fields).items())


# D2 [514] to [525]: HAC FY 2021 in an original and a revised file, FY 2024 with PSI-90, a SIR, a repeated hospital and a
# malformed ID; TPS FY 2025 with fiscal_year and current domains, the 2018 file in the provider_number layout without it (one
# 5-digit ID, the clinical care domain) and the 2019 file with the reviewed odd value.
GROUP_D2 = (
    Stored(
        "cms_cc_hac_reduction_program_hospital",
        "ha1",
        "2021-04-28",
        "FY_2021_HAC_Reduction_Program_Hospital.csv",
        sha("ha1"),
        2,
        records=(hac_row("010001", "2021", "5.5", "No"), hac_row("010003", "2021", "N/A", "N/A")),
    ),
    Stored(
        "cms_cc_hac_reduction_program_hospital",
        "ha2",
        "2021-07-21",
        "FY_2021_HAC_Reduction_Program_Hospital.csv",
        sha("ha2"),
        1,
        records=(hac_row("010001", "2021", "5.7", "No"),),
    ),
    Stored(
        "cms_cc_hac_reduction_program_hospital",
        "ha3",
        "2024-07-31",
        "FY_2024_HAC_Reduction_Program_Hospital.csv",
        sha("ha3"),
        4,
        records=(
            hac_row("010001", "2024", "6.1", "Yes", psi_90_composite_value="1.02", psi_90_w_z_score="0.4", clabsi_sir="0.8"),
            hac_row("010005", "2024", "4.0", "No"),
            hac_row("010005", "2024", "4.2", "No"),
            hac_row("01-005", "2024", "3.0", "No"),
        ),
    ),
    Stored(
        "cms_cc_hvbp_tps",
        "tp1",
        "2025-02-19",
        "hvbp_tps.csv",
        sha("tp1"),
        2,
        records=(
            tps_row(
                "23.5",
                fiscal_year="2025",
                facility_id="010001",
                unweighted_normalized_clinical_outcomes_domain_score="12",
                weighted_person_and_community_engagement_domain_score="5.25",
                unweighted_normalized_safety_domain_score="10",
                weighted_safety_domain_score="2.5",
            ),
            tps_row("Not Available", fiscal_year="2025", facility_id="010003"),
        ),
    ),
    Stored(
        "cms_cc_hvbp_tps",
        "tp0",
        "2019-03-04",
        "hvbp_tps_11_09_2018.csv",
        sha("tp0"),
        2,
        records=(tps_row("38.0", provider_number="010001"), tps_row("30.0", provider_number="10005", unweighted_normalized_clinical_care_domain_score="8")),
    ),
    Stored("cms_cc_hvbp_tps", "tp9", "2020-01-04", "hvbp_tps_12_09_2019.csv", sha("tp9"), 1, records=(tps_row("24.083333333333(23)", facility_id="010001"),)),
)


# AL5 [597] [598]: an OP_32 window equal to 2021 beside a longer one that also covers 2021 (the equal one wins), and a HAC
# FY 2023 row whose HAI measure period is calendar 2021, which places it against the 2021 window.
GROUP_AL5 = (
    Stored(
        "cms_cc_unplanned_hospital_visits_hospital",
        "vi_al5",
        "2022-07-01",
        "Unplanned_Hospital_Visits-Hospital_al5.csv",
        sha("r9"),
        2,
        records=(
            cc(facility_id="010001", measure_id="OP_32", score="11.0", start_date="01/01/2021", end_date="12/31/2021"),
            cc(facility_id="010001", measure_id="OP_32", score="11.5", start_date="07/01/2020", end_date="06/30/2022"),
        ),
    ),
    Stored(
        "cms_cc_hac_reduction_program_hospital",
        "ha_al5",
        "2022-12-01",
        "FY_2023_HAC_Reduction_Program_Hospital.csv",
        sha("q6"),
        1,
        records=(hac_row("010001", "2023", "4.0", "No", hai_measures_start_date="01/01/2021", hai_measures_end_date="12/31/2021"),),
    ),
)

ALZHEIMERS = "Alzheimer's Disease, Related Disorders, or Senile Dementia"
AMI = "Acute Myocardial Infarction"


def mmd_row(
    year: str, condition: str, fips: str, county: str, state: str, urban: str, band: str, value: str, geography: str = "County"
) -> tuple[tuple[str, str], ...]:
    """Return one MMD row with the one filter set the captures hold."""
    fields = {"year": year, "geography": geography, "domain": "Primary chronic conditions", "condition": condition, "fips": fips}
    return tuple((fields | {"county": county, "state": state, "urban": urban, "primary_denominator": band, "analysis_value": value}).items())


def hpsa_row(
    hpsa_id: str, score: str, status: str, designated: str, updated: str, withdrawn: str, geography: str, county: str, **fields: str
) -> tuple[tuple[str, str], ...]:
    """Return one primary-care HPSA component row; the county code is published in two columns."""
    base = {
        "hpsa_name": f"HPSA {hpsa_id}",
        "hpsa_id": hpsa_id,
        "designation_type": "Geographic HPSA",
        "hpsa_discipline_class": "Primary Care",
        "hpsa_score": score,
        "hpsa_status": status,
        "hpsa_designation_date": designated,
        "hpsa_designation_last_update_date": updated,
        "withdrawn_date": withdrawn,
        "hpsa_geography_identification_number": geography,
        "hpsa_component_type_description": "Single County",
        "state_and_county_federal_information_processing_standard_code": county,
        "common_state_county_fips_code": county,
        "state_fips_code": county[:2] if county[:2].isdigit() else "09",
    }
    return tuple((base | fields).items())


def mua_row(
    mua_id: str, code: str, status: str, designated: str, updated: str, withdrawn: str, score: str, name: str, county: str, **fields: str
) -> tuple[tuple[str, str], ...]:
    """Return one MUA or MUP component row."""
    base = {
        "mua_p_id": mua_id,
        "designation_type_code": code,
        "designation_type": "Medically Underserved Area" if code == "MUA" else "Medically Underserved Population",
        "mua_p_status_description": status,
        "designation_date": designated,
        "mua_p_update_date": updated,
        "medically_underserved_area_population_mua_p_withdrawal_date": withdrawn,
        "imu_score": score,
        "medically_underserved_area_population_mua_p_component_geographic_name": name,
        "medically_underserved_area_population_mua_p_component_geographic_type_description": "Single County",
        "state_and_county_federal_information_processing_standard_code": county,
        "state_fips_code": county[:2] if county[:2].isdigit() else "09",
    }
    return tuple((base | fields).items())


HPSA_REPEATED = hpsa_row("101", "15", "Designated", "08/13/2013", "07/02/2018", "", "01001", "01001")
MUA_REPEATED = mua_row("00001", "MUA", "Designated", "1994-01-01", "1994-01-01", "", "52.90", "Autauga", "01001")
# C5 [468] to [480]: MMD county rows with a 4-digit code, a zero, Connecticut and an unknown county; a state-level rate per
# 100,000; C258.01 from an earlier capture whose name has no control. HPSA and MUA with an exact repeat, a reused ID with two
# designations, an MUP and an MUA under one ID and date, an XXXXX county code and a withdrawal without a date.
GROUP_C5 = (
    Stored(
        "cms_mmd_csv",
        "mm02",
        "MMD_API_C258_02_2023__x",
        "mmd_ffs_county_c258_02_prevalence_2023.csv",
        sha("mm02"),
        4,
        records=(
            mmd_row("2023", ALZHEIMERS, "1001", "Autauga County", "ALABAMA", "Urban", "1,000-4,999", "12.5"),
            mmd_row("2023", ALZHEIMERS, "1003", "Baldwin County", "ALABAMA", "Rural", "11-499", "0"),
            mmd_row("2023", ALZHEIMERS, "9001", "Fairfield County", "CONNECTICUT", "Urban", "10,000+", "10.1"),
            mmd_row("2023", ALZHEIMERS, "9990", "", "CONNECTICUT", "", "11-499", "3.0"),
        ),
    ),
    # AL4a [604]: a 2020 county value for 010001's county (01073) before its 2021 window.
    Stored(
        "cms_mmd_csv",
        "mm02_al4",
        "MMD_API_C258_02_2020__x",
        "mmd_ffs_county_c258_02_prevalence_2020.csv",
        sha("q7"),
        1,
        records=(mmd_row("2020", ALZHEIMERS, "1073", "Jefferson County", "ALABAMA", "Urban", "10,000+", "11.0"),),
    ),
    Stored(
        "cms_mmd_csv",
        "mm78",
        "MMD_API_C258_78_2023__x",
        "mmd_ffs_state_c258_78_prevalence_2023.csv",
        sha("mm78"),
        1,
        records=(mmd_row("2023", "Sickle Cell Disease", "1", "", "ALABAMA", "", "10,000+", "170", "State/Territory"),),
    ),
    Stored(
        "cms_mmd_csv",
        "mmami",
        "MMD__x",
        "mmd_ffs_county_ami_prevalence_2022.csv",
        sha("mmami"),
        1,
        records=(mmd_row("2022", AMI, "01001", "Autauga County", "ALABAMA", "Urban", "1,000-4,999", "0.8"),),
    ),
    Stored(
        "hrsa_hpsa_detail",
        "hp1",
        "HPSA__20260924T060817Z__fixture",
        "BCD_HPSA_FCT_DET_PC.csv",
        sha("hp1"),
        9,
        records=(
            HPSA_REPEATED,
            HPSA_REPEATED,
            hpsa_row("102", "7", "Withdrawn", "10/08/2008", "06/27/2013", "06/27/2013", "01003", "01003"),
            hpsa_row("102", "14", "Withdrawn", "08/13/2013", "07/02/2018", "07/02/2018", "01003", "01003"),
            hpsa_row(
                "103",
                "20",
                "Designated",
                "01/05/2022",
                "01/05/2022",
                "",
                "09110010100",
                "XXXXX",
                hpsa_component_type_description="Census Tract",
            ),
            hpsa_row("104", "3", "Withdrawn", "02/01/2000", "02/01/2010", "", "01005", "01005"),
            # [636] A withdrawal without a date in 010001's county holds that county's HPSA values.
            hpsa_row("108", "9", "Withdrawn", "05/01/2015", "05/01/2015", "", "01073", "01073"),
            # [626] A tract ID from another state than the published one stays without a county.
            hpsa_row("106", "12", "Designated", "01/05/2022", "01/05/2022", "", "25025000100", "XXXXX", hpsa_component_type_description="Census Tract"),
            # [627] A facility with a point and a postal code: the county through HUD in its own state.
            hpsa_row(
                "107",
                "18",
                "Designated",
                "01/05/2022",
                "01/05/2022",
                "",
                "POINT (-86.8 33.5)",
                "XXX",
                designation_type="Rural Health Clinic",
                hpsa_component_type_description="Unknown",
                state_fips_code="25",
                hpsa_postal_code="01001",
                primary_state_abbreviation="MA",
            ),
        ),
    ),
    Stored(
        "hrsa_mua_detail",
        "mu1",
        "MUA__20260924T060855Z__fixture",
        "MUA_DET.csv",
        sha("mu1"),
        6,
        records=(
            MUA_REPEATED,
            MUA_REPEATED,
            mua_row("00001", "MUA", "Withdrawn", "1978-11-01", "2001-01-26", "2001-01-26", "44.10", "Autauga", "01001"),
            mua_row("00518", "MUP", "Withdrawn", "2001-11-22", "2009-02-26", "2009-02-26", "50.30", "Pasco", "12101"),
            mua_row("00518", "MUA", "Designated", "2001-11-22", "2009-12-15", "", "54.90", "Pasco", "12101"),
            mua_row(
                "00700",
                "MUA",
                "Designated",
                "2005-10-28",
                "2005-10-28",
                "",
                "61.30",
                "113.02",
                "XXXXX",
                medically_underserved_area_population_mua_p_component_geographic_type_description="Census Tract",
            ),
        ),
    ),
)


def owner_case(**fields: str) -> Stored:
    """Return one more owner file with one row, for the failing owner cases."""
    return Stored(
        "cms_hospital_owners",
        "ow9",
        "CMS_OWNERS_ORG__fixture_x",
        "organisation_owners.csv",
        sha("o9"),
        1,
        records=(owner("O20000000009", "2222222222", "34", **fields),),
    )


def impact_case(content: tuple[str, ...], member: str = "FY 2026 IPPS Proposed Rule Impact File.txt") -> Stored:
    """Return a FY 2026 impact file with the given lines, for the failing impact cases."""
    return Stored("cms_ipps_text_lines", "p09", "CMS_IPPS__r", member, sha("im9"), len(content), content=content)


def cmi_case(content: tuple[str, ...]) -> Stored:
    """Return a FY 2026 final CMI file with the given lines, for the failing CMI cases."""
    return Stored(
        "cms_ipps_text_lines",
        "r2",
        "main-cmi-ipps__r2",
        "FY24 CMIs - V41 Billed DRGs (FR 2026).txt",
        sha("r2"),
        len(content),
        "main-cmi-ipps__r2",
        content=content,
    )


OM_HEADER = "PROV\tFROM\tTO\t RNSAL \t RNHR \tLPNSTHR\tNAORATHR\tMAHR\tnursehr\trnahw"
OM_ROWS = (
    OM_HEADER,
    '01014F\t1/1/2022\t12/31/2022\t" 1,200 "\t100\t20\t25\t5\t150\t12',
    "010002\t12/25/2021\t12/24/2022\t0\t0\t0\t0\t0\t0\t.",
    "010003\t1/1/2022\t12/31/2022\t-\t.\t20\t25\t5\t.\t.",
    "010006\t1/1/2022\t12/31/2022\t1200\t100\t20\t25\t5\t150\t12",
    "010006\t1/1/2022\t12/31/2022\t1200\t100\t20\t25\t5\t150\t12",
    "BAD\t1/1/2022\t12/31/2022\t1200\t100\t20\t25\t5\t150\t12",
    "010008\t1/1/2023\t12/31/2022\t1200\t100\t20\t25\t5\t150\t12",
    "010009\tinvalid\t12/31/2022\t1200\t100\t20\t25\t5\t150\t12",
)

BASE = (
    Stored(
        "cms_occupational_mix_sheet_rows",
        "om05",
        "CMS_OCCMIX__om5",
        "FY26_CBSAOccMix_NoOccMix.xlsx",
        sha("ox5"),
        4,
        content=(
            "CBSAGEO_AHW_w_wo_OccMix:CBSA|HOURS",
            "CBSAGEO_AHW_w_wo_OccMix:12345|100",
            "Final_Rule_OccMix_Factor:PROV|FROM|TO|FACTOR",
            "Final_Rule_OccMix_Factor:010001|1/1/2022|12/31/2022|1.1",
        ),
    ),
    Stored(
        "cms_occupational_mix_text_lines_utf16",
        "om03",
        "CMS_OCCMIX__om3",
        "FY22_Final_OccMix_PUF.txt",
        sha("om3"),
        3,
        content=(
            "Occupational Mix Survey",
            "RNHR\tPROV\tFROM\tTO\tRNSAL\tLPNSTHR\tNAORATHR\tMAHR\tnursehr\trnahw",
            "80\t010004\t1/1/2019\t12/31/2019\t1600\t10\t5\t5\t100\t20",
        ),
    ),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "om04",
        "CMS_OCCMIX__om4",
        "FY19_Final_OccMix_PUF.xlsx",
        sha("om4"),
        2,
        content=(
            "OccMix:PROV|FROM|TO|RNSAL|RNHR|LPNSTHR|NAORATHR|MAHR|nursehr|rnahw",
            "OccMix:10005|2016-01-01T00:00:00|2016-12-31T00:00:00|3000|100|0|0|0|100|30",
        ),
    ),
    Stored("cms_occupational_mix_text_lines", "om01", "CMS_OCCMIX__om", "FY26_Final_OccMix_PUF.txt", sha("om1"), len(OM_ROWS), content=OM_ROWS),
    Stored(
        "cms_occupational_mix_text_lines",
        "om02",
        "CMS_OCCMIX__om",
        "FY26_Final_Survey_PUF.txt",
        sha("om2"),
        2,
        content=(
            OM_HEADER.replace("PROV", "PROV\tIs Prov on this Occupational Mix Delete List because its Occupational Mix Data is Aberrant? (Y/N)"),
            OM_ROWS[1].replace("01014F", "01014F\tY"),
        ),
    ),
    # HAI: a release republished byte for byte [171]; the year-to-date archive dated by capture [170]; the canonical
    # copy is the smallest object key, here the archive copy, never the earliest or latest capture [167].
    # HAI rows are "entity|measure|start|end|score". A later release revises a value; the latest dated file wins [231].
    Stored("cms_hai_hospital", "h01", "2021-01-27", HAI_FILE, sha("a1"), 3, content=HAI_2021),
    Stored("cms_hai_hospital", "h02", "2021-03-31", HAI_FILE, sha("a1"), 3, content=HAI_2021),
    Stored("cms_hai_hospital", "h03", "2026-05-13", HAI_FILE, sha("a2"), 2, content=HAI_2026_MAY),
    Stored("cms_hai_hospital", "h00", "2026-08-19", f"hospitals_2026-05-13.zip!{HAI_FILE}", sha("a2"), 2, content=HAI_2026_MAY),
    Stored("cms_hai_hospital", "h04", "2026-08-19", f"hospitals_2026-08-13.zip!{HAI_FILE}", sha("a3"), 4, content=HAI_2026_AUG),
    # A notes file packed with the HAI tables has no key [235]; another file on the same date conflicts for one key [232].
    Stored("cms_hai_hospital", "h05", "2026-08-19", "readme_bundle.zip!Notes.csv", sha("a4"), 1, content=("||||",)),
    Stored("cms_hai_hospital", "h07", "2026-08-13", "HAI_Hospital_Supplement.csv", sha("a5"), 1, content=("010003|HAI_1_SIR|01/01/2025|12/31/2025|0.45",)),
    Stored("cms_hai_state", "s01", "2021-01-27", "Healthcare_Associated_Infections-State.csv", sha("b1"), 2, content=HAI_STATE),
    Stored("cms_hai_national", "n01", "2021-01-27", "Healthcare_Associated_Infections-National.csv", sha("b2"), 1, content=HAI_NATIONAL),
    # Cost reports: one file captured in two snapshots on the same day.
    Stored("cms_hospital_cost_reports", "c01", "CMS_HCRIS_PUF__20260924T040326Z__aa", "CostReport_2023_Final.csv", sha("c1"), 3, "snap-aa", records=COSTS_2023),
    Stored("cms_hospital_cost_reports", "c02", "CMS_HCRIS_PUF__20260924T042740Z__bb", "CostReport_2023_Final.csv", sha("c1"), 3, "snap-bb", records=COSTS_2023),
    Stored("cms_hospital_cost_reports", "c03", "CMS_HCRIS_PUF__20260924T040326Z__aa", "CostReport_2022_Final.csv", sha("c2"), 2, "snap-aa", records=COSTS_2022),
    # IPPS: the two BRZ-016 files under conflicting names, held [172]; one ordinary file.
    Stored("cms_ipps_text_lines", "i01", "CMS_IPPS__a", "FY 2019 IPPS Proposed Rule Impact File.txt", next(iter(HELD)), 2, content=HELD_TEXT),
    Stored(
        "cms_ipps_text_lines",
        "i02",
        "CMS_IPPS__a",
        "FY 2019 IPPS Proposed Rule Impact File (Variable Descriptions).txt",
        next(iter(HELD)),
        2,
        content=HELD_TEXT,
    ),
    # Its workbook twin: the text is held, so no selected text covers the data sheet and it is kept [268]; a sheet with no
    # data rows cannot be compared and is kept [274].
    Stored(
        "cms_ipps_sheet_rows",
        "w05",
        "CMS_IPPS__a",
        "FY 2019 IPPS Proposed Rule Impact File.xlsx",
        sha("n2"),
        3,
        content=("FY19 NPRM:Provider Number|CMI", "FY19 NPRM:10001|1.5", "Variable Descriptions:Variable|Meaning"),
    ),
    Stored("cms_ipps_text_lines", "i03", "CMS_IPPS__b", "FY 2020 Correction Notice Impact File.txt", list(HELD)[1], 3),
    Stored("cms_ipps_text_lines", "i04", "CMS_IPPS__c", "FY 2019 IPPS FR and CN Impact File (CN data).txt", list(HELD)[1], 3),
    # Twins that agree [185] [189] to [192]: a title line, CSV quoting, a leading zero, $ and separators, displayed decimals,
    # percentages, a value exactly at the half-unit boundary and a space inside quotes [200].
    Stored("cms_ipps_text_lines", "i05", "CMS_IPPS__c", "FY 2021 Final Rule Impact File.txt", sha("d1"), 4, content=TWIN_TEXT),
    Stored("cms_ipps_sheet_rows", "w01", "CMS_IPPS__c", "FY 2021 Final Rule Impact File.xlsx", sha("d2"), 4, content=TWIN_SHEET),
    # A twin pair that agrees on the final-rule sheet; the correction-notice sheet matches no selected text and is kept [267].
    Stored("cms_ipps_text_lines", "i11", "CMS_IPPS__m", "FY 2024 Final Rule Impact File.txt", sha("l1"), 4, content=TWIN_TEXT),
    Stored("cms_ipps_sheet_rows", "w06", "CMS_IPPS__m", "FY 2024 Final Rule Impact File.xlsx", sha("l2"), 6, content=FR_CN_SHEET),
    # Twins whose text file is fixed-width, with thousands separators: not compared, the workbook is preferred [186] [193].
    Stored(
        "cms_ipps_text_lines",
        "i06",
        "CMS_IPPS__c",
        "FY 2022 Final Rule Impact File.txt",
        sha("d4"),
        3,
        content=("PROV  NAME   CMI    WAGES", "010001ALPHA 1.2345 $60,884,976.48", "010002BETA  0.5000 $1,000.00"),
    ),
    Stored(
        "cms_ipps_sheet_rows",
        "w02",
        "CMS_IPPS__c",
        "FY 2022 Final Rule Impact File.xlsx",
        sha("d5"),
        3,
        content=("Data:PROV|NAME|CMI", "Data:10001|ALPHA|1.2345", "Data:10002|BETA|0.5"),
    ),
    Stored("cms_ipps_sas", "x01", "CMS_IPPS__d", "prds_hosp10_yr2019.sas7bdat", sha("d3"), 2),
    # Occupational mix: one capture packs the same file in two nested archives, neither dated [170].
    # A UTF-16 text file has its own table and still pairs with its workbook [199].
    Stored(
        "cms_occupational_mix_text_lines_utf16",
        "u01",
        "CMS_OCCMIX__b",
        "FY_2017_FINAL_provoccmix.zip!FY_2017_FR_provoccmix_06302016.txt",
        sha("g1"),
        2,
        "CMS_OCCMIX__u",
        content=("PROV\tWAGE", "010001\t$28.20"),
    ),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v02",
        "CMS_OCCMIX__b",
        "FY_2017_FINAL_provoccmix.zip!FY_2017_FR_provoccmix_06302016.xlsx",
        sha("g2"),
        2,
        "CMS_OCCMIX__u",
        content=("Data:PROV|WAGE", "Data:010001|28.199739"),
    ),
    # Dates as m/d/yyyy in the text and Excel serial numbers or ISO timestamps in the workbook agree [202].
    Stored(
        "cms_occupational_mix_text_lines",
        "m03",
        "CMS_OCCMIX__d",
        "test9mc040513.txt",
        sha("h1"),
        3,
        "CMS_OCCMIX__d",
        content=("PROV\tFROM\tTO\tAHW", "010001\t01/01/2010\t12/31/2010\t28.1252", "010005\t01/04/2010\t12/28/2010\t28.5869"),
    ),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v03",
        "CMS_OCCMIX__d",
        "test9mc040513.xls",
        sha("h2"),
        3,
        "CMS_OCCMIX__d",
        content=("Sheet1:PROV|FROM|TO|AHW", "Sheet1:010001|40179.0|40543.0|28.125204115", "Sheet1:010005|2010-01-04T00:00:00|40540.0|28.586945499"),
    ),
    # A tab-separated header over space-separated data is fixed-width: not compared, the workbook is used [201].
    Stored(
        "cms_ipps_text_lines",
        "i09",
        "CMS_IPPS__g",
        "FY_2012_FINAL_CMI.TXT",
        sha("j1"),
        3,
        "CMS_IPPS__g",
        content=("PROV\tCMI", "010001 01.695408", "010005 01.236252"),
    ),
    Stored(
        "cms_ipps_sheet_rows",
        "w03",
        "CMS_IPPS__g",
        "FY_2012_FINAL_CMI.xlsx",
        sha("j2"),
        3,
        "CMS_IPPS__g",
        content=("Data:PROV|CMI", "Data:010001|1.695408", "Data:010005|1.236252"),
    ),
    # A text file and workbook under different names, paired by a reviewed override, agree like a same-name pair [218] [220].
    Stored("cms_ipps_text_lines", "i10", "CMS_IPPS__h", "FY 2023 Final Rule Impact File.txt", sha("k1"), 4, "CMS_IPPS__h", content=TWIN_TEXT),
    Stored("cms_ipps_sheet_rows", "w04", "CMS_IPPS__h", "IMPACT_FY23_FR_PUF.xlsx", sha("k2"), 4, "CMS_IPPS__h", content=TWIN_SHEET),
    # An occupational-mix workbook whose second sheet is published as its own text file in the same release: each sheet is
    # covered by the selected text that matches it [270] [276].
    Stored("cms_occupational_mix_text_lines", "m04", "CMS_OCCMIX__p", "FY26_S3_PUF.txt", sha("p1"), 2, "CMS_OCCMIX__p", content=("PROV\tS3", "010001\t100")),
    Stored("cms_occupational_mix_text_lines", "m05", "CMS_OCCMIX__p", "FY26_AHW_PUF.txt", sha("p3"), 2, "CMS_OCCMIX__p", content=("PROV\tAHW", "010001\t0.5")),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v04",
        "CMS_OCCMIX__p",
        "FY26_S3_PUF.xlsx",
        sha("p2"),
        4,
        "CMS_OCCMIX__p",
        content=("S-3 Data:PROV|S3", "S-3 Data:010001|100", "AHW Data:PROV|AHW", "AHW Data:010001|0.5"),
    ),
    # The text twin is comma-separated and one value differs from its workbook: the pair differs and both are held [187].
    Stored("cms_occupational_mix_text_lines", "m01", "CMS_OCCMIX__a", "PUFs.zip!AHW_by_Provider.zip!provcbsaahw.txt", sha("e1"), 3, content=OCCMIX_TEXT),
    Stored("cms_occupational_mix_text_lines", "m02", "CMS_OCCMIX__a", "PUFs.zip!provcbsaahw.zip!provcbsaahw.txt", sha("e1"), 3, content=OCCMIX_TEXT),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v01",
        "CMS_OCCMIX__a",
        "PUFs.zip!provcbsaahw.xlsx",
        sha("e2"),
        3,
        content=("Sheet1:Provider|Wage", "Sheet1:10001|3.6", "Sheet1:10002|4"),
    ),
    *GROUP_A,
    *GROUP_B,
    *GROUP_B3,
    *GROUP_B4,
    *GROUP_B5A,
    *GROUP_B5B,
    *GROUP_C1,
    *GROUP_C2,
    *GROUP_C3,
    *GROUP_C4,
    *GROUP_C5,
    *GROUP_D1,
    *GROUP_D2,
    *GROUP_AL1,
    *GROUP_AL2,
    *GROUP_AL3B,
    *GROUP_AL5,
)
# Each failing case changes the base fixture, or drops label and period rows, and names the one dbt test that must catch it.
FAILING: dict[str, tuple[str, tuple[Stored, ...], frozenset[str]]] = {
    "occmix_uncast_value": (
        "assert_occmix_values_cast",
        tuple(replace(item, content=(OM_HEADER, OM_ROWS[1].replace("1,200", "not-a-number"), *OM_ROWS[2:])) if item.key == "om01" else item for item in BASE),
        frozenset(),
    ),
    "occmix_malformed_grouping": (
        "assert_occmix_values_cast",
        tuple(replace(item, content=tuple(line.replace("1,200", "12,00") for line in item.content)) if item.key == "om01" else item for item in BASE),
        frozenset(),
    ),
    "occmix_unknown_layout": (
        "assert_occmix_layouts_known",
        tuple(
            replace(
                item,
                content=tuple(
                    line.replace("RNSAL", "unknown_salary")
                    .replace("RNHR", "unknown_hours")
                    .replace("nursehr", "unknown_total")
                    .replace("LPNSTHR", "unknown_lpnst")
                    .replace("NAORATHR", "unknown_naorat")
                    .replace("MAHR", "unknown_ma")
                    for line in item.content
                ),
            )
            if item.key == "om01"
            else item
            for item in BASE
        ),
        frozenset(),
    ),
    "name_clash": ("assert_no_release_name_clash", (*BASE, Stored("cms_hai_hospital", "h06", "2021-01-27", HAI_FILE, sha("a9"), 2)), frozenset()),
    # Bronze holds rows of a copy the copies table lists as not loaded [207] [209].
    "copy_rows_in_bronze": (
        "assert_file_copies_match_bronze_objects",
        tuple(replace(item, force_loaded=True) if item.key == "c02" else item for item in BASE),
        frozenset(),
    ),
    "stale_hold": ("assert_label_holds_name_stored_files", tuple(item for item in BASE if item.sha != list(HELD)[1]), frozenset()),
    "two_checksums": (
        "assert_file_copies_one_checksum_per_object",
        tuple(replace(item, second_sha=sha("f1")) if item.key == "c03" else item for item in BASE),
        frozenset(),
    ),
    # Copies of one file named for two fiscal years, with no owner hold [181].
    "unheld_conflict": (
        "assert_label_conflicts_are_held",
        (
            *BASE,
            Stored("cms_ipps_text_lines", "i07", "CMS_IPPS__e", "FY 2017 Final Rule Impact File.txt", sha("d9"), 1),
            Stored("cms_ipps_text_lines", "i08", "CMS_IPPS__f", "FY 2018 Final Rule Impact File.txt", sha("d9"), 1),
        ),
        frozenset(),
    ),
    # The label map misses one loaded copy [182].
    "unlabelled_copy": ("assert_file_labels_cover_copies", BASE, frozenset({"i05"})),
    # The period seed misses one POS file [281].
    "pos_missing_period": ("assert_pos_files_have_periods", BASE, frozenset({"pb"})),
    # A POS bed count that is not a number [288].
    "pos_uncast_value": (
        "assert_pos_values_cast",
        tuple(with_record(item, pos("010003", state_cd="AL", bed_cnt="12a")) if item.key == "pb" else item for item in BASE),
        frozenset(),
    ),
    # One CCN twice in one POS file [283].
    "pos_duplicate_ccn": (
        "unique_int_pos_hospital_snapshots_snapshot_key",
        tuple(with_record(item, pos("010002", state_cd="AL")) if item.key == "pb" else item for item in BASE),
        frozenset(),
    ),
    # A file of a CMI snapshot whose family is neither read as CMI nor reviewed as not CMI [293].
    "cmi_unreviewed_family": (
        "assert_cmi_families_reviewed",
        (*BASE, replace(cmi_case((CMI_HEADER, "010001\t1.5\t10\t15")), member="FY 2020 CMI Extra Table.txt")),
        frozenset(),
    ),
    # A CMI file with no CMI column [297].
    "cmi_unknown_layout": ("assert_cmi_files_have_layout", (*BASE, cmi_case(("Prov\tIndex", "010001\t1.5"))), frozenset()),
    # A CMI column that holds the transfer-adjusted values [294].
    "cmi_transfer_adjusted": ("assert_cmi_matches_relative_weights", (*BASE, cmi_case((CMI_HEADER, "010001\t1.6735\t8178\t13823.43"))), frozenset()),
    # A CMI outside the plausible range [303].
    "cmi_out_of_range": ("assert_cmi_values_plausible", (*BASE, cmi_case((CMI_HEADER, "010001\t12.5\t\t"))), frozenset()),
    # A cost report whose period starts in another fiscal year than its file [332].
    "cost_report_wrong_year": (
        "assert_cost_report_fiscal_years",
        tuple(
            with_record(item, cc(rpt_rec_num="090003", provider_ccn="010009", fiscal_year_begin_date="01/01/2020", fiscal_year_end_date="12/31/2020"))
            if item.key == "c03"
            else item
            for item in BASE
        ),
        frozenset(),
    ),
    # A cost-report amount that is not a number [336].
    "cost_report_uncast_value": (
        "assert_cost_report_values_cast",
        tuple(
            with_record(item, cc(rpt_rec_num="090004", provider_ccn="010009", fiscal_year_begin_date="10/01/2021", number_of_beds="12a"))
            if item.key == "c03"
            else item
            for item in BASE
        ),
        frozenset(),
    ),
    # An impact-like family that is neither read nor excluded [342].
    "impact_unreviewed_family": (
        "assert_impact_families_reviewed",
        (*BASE, impact_case(("Provider Number\tBeds", "010001\t10"), member="FY 2026 IPPS Final Rule Impact Supplement.txt")),
        frozenset(),
    ),
    # A read impact file with no header row [343].
    "impact_no_header": ("assert_impact_files_have_layout", (*BASE, impact_case(("010001\tAlpha\t0.9",))), frozenset()),
    # Two headers of one file that map to one field [345].
    "impact_repeated_field": (
        "assert_impact_files_have_layout",
        (*BASE, impact_case(("Provider Number\tMedicare Percentage\tMCR_PCT", "010001\t0.1\t0.1"))),
        frozenset(),
    ),
    # A numeric value that is not a number [349].
    "impact_uncast_value": ("assert_impact_values_cast", (*BASE, impact_case(("Provider Number\tBeds", "010001\t12a"))), frozenset()),
    # An HHS number that is not a plain number, and a week that is not YYYY/MM/DD [375] [376].
    "hhs_value_uncast": (
        "assert_hhs_onc_values_cast",
        tuple(
            with_record(item, cc(hospital_pk="010009", collection_week="2021/01/03", total_beds_7_day_avg="12a")) if item.key == "hh1" else item
            for item in BASE
        ),
        frozenset(),
    ),
    "hhs_week_uncast": (
        "assert_hhs_onc_values_cast",
        tuple(with_record(item, cc(hospital_pk="010009", collection_week="2021-01-17")) if item.key == "hh1" else item for item in BASE),
        frozenset(),
    ),
    # A Promoting Interoperability criterion that is neither Y nor N [380].
    "onc_flag_uncast": (
        "assert_hhs_onc_values_cast",
        tuple(
            with_record(item, cc(facility_id="010009", meets_criteria_for_promoting_interoperability_of_ehrs="Maybe", year="2023"))
            if item.key == "oc1"
            else item
            for item in BASE
        ),
        frozenset(),
    ),
    # An owner row that is not an organisation [367].
    "owner_individual": ("assert_owner_rows_are_organisations", (*BASE, owner_case(type_owner="I")), frozenset()),
    # An owner flag that is neither Y nor N, and a share above 100 [366] [368].
    "owner_flag_uncast": ("assert_ownership_values_cast", (*BASE, owner_case(private_equity_company_owner="X")), frozenset()),
    "owner_share_out_of_range": ("assert_ownership_values_cast", (*BASE, owner_case(percentage_ownership="150")), frozenset()),
    # A change-of-ownership date that is not M/D/YYYY [369].
    "chow_date_uncast": (
        "assert_ownership_values_cast",
        tuple(with_record(item, cc(enrollment_id_buyer="O20000000008", effective_date="2022-07-01")) if item.key == "xw2" else item for item in BASE),
        frozenset(),
    ),
    # An owner file without a recorded period [365].
    "ownership_no_period": ("assert_ownership_files_have_periods", BASE, frozenset({"ow1"})),
    # A Medicare inpatient file whose name has no data year [355].
    "mup_no_data_year": (
        "assert_mup_files_have_data_year",
        (
            *BASE,
            Stored(
                "cms_medicare_inpatient_by_provider",
                "mp9",
                "CMS_MEDICARE_PROVIDER__b",
                "MUP_INP_Prv.CSV",
                sha("mp9"),
                1,
                records=(cc(rndrng_prvdr_ccn="010009", tot_dschrgs="20"),),
            ),
        ),
        frozenset(),
    ),
    # A data year whose DRG cells come from two files, which the sepsis share would sum [356].
    "mup_drg_year_twice": (
        "assert_mup_drg_one_file_per_data_year",
        (
            *BASE,
            Stored(
                "cms_medicare_inpatient_by_drg",
                "md9",
                "CMS_MUP_DRG__b",
                "MUP_INP_RY25_P03_V10_DY23_PrvSvc.CSV",
                sha("md9"),
                1,
                records=(cc(rndrng_prvdr_ccn="010001", drg_cd="871", tot_dschrgs="60"),),
            ),
        ),
        frozenset(),
    ),
    # A Medicare inpatient count that is not a plain number [359].
    "mup_uncast_value": (
        "assert_mup_values_cast",
        tuple(with_record(item, cc(rndrng_prvdr_ccn="010009", tot_benes="1,000")) if item.key == "mp2" else item for item in BASE),
        frozenset(),
    ),
    # [403] A HUD file without its recorded quarter.
    "geography_no_period": ("assert_geography_files_have_periods", BASE, frozenset({"hz2"})),
    # [405] A HUD ratio that is not a number.
    "hud_uncast_ratio": (
        "assert_geography_values_cast",
        tuple(with_record(item, hud("02108", "25025", "abc", "1")) if item.key == "hz2" else item for item in BASE),
        frozenset(),
    ),
    # [406] A ZIP whose residential ratios sum to neither 0 nor 1.
    "hud_ratio_sum_off": (
        "assert_hud_ratios_normalized",
        tuple(
            with_record(with_record(item, hud("02109", "25025", "0.6", "0.6")), hud("02109", "25017", "0.2", "0.4")) if item.key == "hz2" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [408] An edge without its reciprocal.
    "adjacency_one_way": (
        "assert_adjacency_edges_reciprocal",
        tuple(with_record(item, adjacent("Autauga County, AL", "01001", "Elmore County, AL", "01051", "500")) if item.key == "aj1" else item for item in BASE),
        frozenset(),
    ),
    # [412] A 2013 RUCC sheet whose header changed.
    "rucc_unknown_layout": (
        "assert_geography_layouts_known",
        tuple(replace(item, content=(item.content[0].replace("RUCC_2013", "RUCC_Code"), *item.content[1:])) if item.key == "rs1" else item for item in BASE),
        frozenset(),
    ),
    # [411] A secondary RUCA code outside the published labels.
    "ruca_unknown_code": (
        "assert_geography_values_cast",
        tuple(
            with_record(item, cc(zipcode="99951", state="AK", zipcodetype="ZIP Code Area", primaryruca="4", secondaryruca="4.7")) if item.key == "rz1" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [413] A service-area count that is neither a number nor the suppression mark.
    "hsa_uncast_count": (
        "assert_geography_values_cast",
        tuple(with_record(item, hsa("010001", "32423", "12a", "60", "500")) if item.key == "hs2" else item for item in BASE),
        frozenset(),
    ),
    # [420] A mapped column whose published label changed.
    "acs_label_changed": (
        "assert_acs_labels_match_map",
        tuple(replace(item, labels=(("GEO_ID", "Geography"), ("DP04_0078PE", DP04_LABELS["1.51 or more"]))) if item.key == "a23" else item for item in BASE),
        frozenset(),
    ),
    # [423] A loaded vintage the map does not cover.
    "acs_vintage_unmapped": (
        "assert_acs_map_covers_files",
        (
            *BASE,
            Stored(
                "acs_dp04",
                "a24",
                "ACS__d24",
                "ACSDP5Y2024.DP04-Data.csv",
                sha("a24"),
                1,
                records=(acs("0500000US01001", dp04_0078pe="3.3"),),
                labels=(("GEO_ID", "Geography"), ("DP04_0078PE", DP04_LABELS["1.01 to 1.50"])),
            ),
        ),
        frozenset(),
    ),
    # [426] An ACS value that is neither a number nor a published token.
    "acs_value_uncast": (
        "assert_county_context_values_cast",
        tuple(with_record(item, acs("0500000US01003", s1701_c03_001e="12a")) if item.key == "s23" else item for item in BASE),
        frozenset(),
    ),
    # [428] An SVI field that is neither a number nor -999.
    "svi_value_uncast": (
        "assert_county_context_values_cast",
        tuple(with_record(item, svi_row(st="01", fips="01003", ep_pov150="abc")) if item.key == "sv22" else item for item in BASE),
        frozenset(),
    ),
    # [439] A SAHIE file whose preamble no longer defines code 0 as under 65.
    "sahie_category_changed": (
        "assert_sahie_files_consistent",
        tuple(
            replace(item, preamble=tuple(line.replace("Under 65 years", "Under 19 years") for line in item.preamble)) if item.key == "sh22" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [438] A SAHIE row whose year differs from its file's.
    "sahie_year_mismatch": (
        "assert_sahie_files_consistent",
        tuple(with_record(item, sahie_row("01003", year="2021", nipr="100")) if item.key == "sh22" else item for item in BASE),
        frozenset(),
    ),
    # [435] A SAIPE county row whose numeric positions hold text.
    "saipe_value_uncast": (
        "assert_income_labor_values_cast",
        tuple(
            replace(item, rows=3, content=(*item.content, saipe_line("01", "5", saipe_values("12a", "1", "2", "3", "4", "5", "6", "7", "8"), "Barbour County")))
            if item.key == "sp99"
            else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [445] A BLS measure code outside the four LAUS measures.
    "bls_unknown_measure": (
        "assert_income_labor_values_cast",
        tuple(with_record(item, bls_row("09", "2023", "M01", "1")) if item.key == "bl1" else item for item in BASE),
        frozenset(),
    ),
    # [515] A TPS file with neither fiscal_year nor a reviewed year.
    "tps_undated": (
        "assert_program_years_dated",
        (*BASE, Stored("cms_cc_hvbp_tps", "tpx", "2018-01-04", "hvbp_tps_12_01_2017.csv", sha("tpx"), 1, records=(tps_row("37.0", facility_id="010001"),))),
        frozenset(),
    ),
    # [551] An outcome value that is neither a number nor a published token.
    "outcome_value_uncast": (
        "assert_hai_outcome_values_cast",
        (*BASE, Stored("cms_hai_hospital", "hx1", "2022-06-01", "HAI_Bad_Value.csv", sha("ax1"), 1, content=("990001|HAI_2_SIR|01/01/2021|12/31/2021|1.2a",))),
        frozenset(),
    ),
    # [553] A SIR that is not observed / predicted.
    "outcome_ratio_mismatch": (
        "assert_hai_outcome_consistent",
        (
            *BASE,
            Stored(
                "cms_hai_hospital",
                "hx2",
                "2022-06-01",
                "HAI_Bad_Ratio.csv",
                sha("ax2"),
                3,
                content=(
                    "990001|HAI_3_SIR|01/01/2021|12/31/2021|2.000",
                    "990001|HAI_3_NUMERATOR|01/01/2021|12/31/2021|3",
                    "990001|HAI_3_ELIGCASES|01/01/2021|12/31/2021|2.000",
                ),
            ),
        ),
        frozenset(),
    ),
    # [553] A SIR outside its published bounds.
    "outcome_outside_bounds": (
        "assert_hai_outcome_consistent",
        (
            *BASE,
            Stored(
                "cms_hai_hospital",
                "hx3",
                "2022-06-01",
                "HAI_Bad_Bounds.csv",
                sha("ax3"),
                2,
                content=("990001|HAI_5_SIR|01/01/2021|12/31/2021|1.000", "990001|HAI_5_CILOWER|01/01/2021|12/31/2021|1.200"),
            ),
        ),
        frozenset(),
    ),
    # [550] A calendar-year HAI measure ID outside the 36 known parts.
    # [569] A C141 spelling outside the accepted list must fail, never fold into a category.
    "care_compare_edv_unknown": (
        "assert_care_compare_edv_known",
        tuple(with_record(item, cc(facility_id="010001", measure_id="EDV", score="LOW", **DATES_2022)) if item.key == "te_al2a" else item for item in BASE),
        frozenset(),
    ),
    "outcome_measure_unknown": (
        "assert_hai_outcome_measures_known",
        (*BASE, Stored("cms_hai_hospital", "hx4", "2022-06-01", "HAI_Bad_Measure.csv", sha("ax4"), 1, content=("990001|HAI_7_SIR|01/01/2021|12/31/2021|0.5",))),
        frozenset(),
    ),
    # [519] A HAC score that is not a number, a token or the reviewed value.
    "hac_value_uncast": (
        "assert_validation_values_cast",
        tuple(with_record(item, hac_row("010007", "2024", "5..5", "No")) if item.key == "ha3" else item for item in BASE),
        frozenset(),
    ),
    # [510] A visits score that is neither a number nor a published token.
    "validation_value_uncast": (
        "assert_validation_values_cast",
        tuple(with_record(item, cc(facility_id="010005", measure_id="OP_36", score="1O.5", **DATES_2022)) if item.key == "vi1" else item for item in BASE),
        frozenset(),
    ),
    # [510] An HRRP count with an unknown token.
    "hrrp_token_unknown": (
        "assert_validation_values_cast",
        tuple(with_record(item, hrrp_row("010005", "READM-30-PN-HRRP", "0.98", "NA*")) if item.key == "hr1" else item for item in BASE),
        frozenset(),
    ),
    # [468] An MMD label the reviewed map does not name.
    "mmd_label_unmapped": (
        "assert_mmd_controls_match_labels",
        tuple(
            with_record(item, mmd_row("2023", "Unreviewed Condition", "1005", "Barbour County", "ALABAMA", "Rural", "500-999", "4.2"))
            if item.key == "mm02"
            else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [468] A file named for one control holding another control's label.
    "mmd_file_control_disagrees": (
        "assert_mmd_controls_match_labels",
        tuple(
            with_record(item, mmd_row("2023", AMI, "1005", "Barbour County", "ALABAMA", "Rural", "500-999", "1.1")) if item.key == "mm02" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [469] An MMD value that is not a number.
    "mmd_value_uncast": (
        "assert_shortage_values_cast",
        tuple(
            with_record(item, mmd_row("2023", ALZHEIMERS, "1005", "Barbour County", "ALABAMA", "Rural", "500-999", "x")) if item.key == "mm02" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [476] Two different HPSA rows for one ID, designation date and geography.
    "hpsa_repeated_grain": (
        "assert_shortage_grain",
        tuple(
            with_record(item, hpsa_row("101", "16", "Designated", "08/13/2013", "07/02/2018", "", "01001", "01001")) if item.key == "hp1" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [478] An HPSA date in another format.
    "hpsa_date_uncast": (
        "assert_shortage_values_cast",
        tuple(
            with_record(item, hpsa_row("105", "9", "Designated", "2013-08-13", "07/02/2018", "", "01007", "01007")) if item.key == "hp1" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [479] An MUA score that is not a number.
    "mua_score_uncast": (
        "assert_shortage_values_cast",
        tuple(
            with_record(item, mua_row("00701", "MUA", "Designated", "2005-10-28", "2005-10-28", "", "n/a", "Baldwin", "01003")) if item.key == "mu1" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [476] Two different MUA rows for one component of one designation.
    "mua_repeated_grain": (
        "assert_shortage_grain",
        tuple(
            with_record(item, mua_row("00518", "MUA", "Designated", "2001-11-22", "2009-12-15", "", "55.00", "Pasco", "12101")) if item.key == "mu1" else item
            for item in BASE
        ),
        frozenset(),
    ),
    # [457] A repeated WONDER county-year whose values differ between exports.
    "wonder_repeat_differs": (
        "assert_wonder_repeats_identical",
        tuple(replace(item, records=(wonder_row("01001", "2024", "521", "Unreliable"),)) if item.key == "wd3" else item for item in BASE),
        frozenset(),
    ),
    # [451] A PLACES release with two values for one county, measure, value type and data year.
    "places_repeated_grain": (
        "assert_county_health_grain",
        tuple(with_record(item, places_row("2023", "AL", "01001", "DIABETES", "12.4")) if item.key == "pl25" else item for item in BASE),
        frozenset(),
    ),
    # [455] A PLACES value that is not a number.
    "places_value_uncast": (
        "assert_county_health_values_cast",
        tuple(with_record(item, places_row("2023", "AL", "01005", "DIABETES", "12a")) if item.key == "pl25" else item for item in BASE),
        frozenset(),
    ),
    # [455] A geographic variation value that is neither a number nor *.
    "gv_value_uncast": (
        "assert_county_health_values_cast",
        tuple(with_record(item, gv_row("2021", "County", "01001", ma_prtcptn_rate="x")) if item.key == "gv1" else item for item in BASE),
        frozenset(),
    ),
    # An emergency-services value that is neither Yes nor No [329].
    "hgi_uncast_value": (
        "assert_hgi_values_cast",
        tuple(with_record(item, (("facility_id", "010009"), ("emergency_services", "Maybe"))) if item.key == "g2" else item for item in BASE),
        frozenset(),
    ),
}
# Reviewed pairs under different names, as committed in the overrides file [220].
FIXTURE_RENAMED = ((sha("k1"), sha("k2")),)
# The container name the fixture objects stand in, so names without a year still get one [178].
FIXTURE_CONTAINER = "FY_2021_fixture.zip"
TEXT_TABLES = {
    "cms_ipps_text_lines",
    "cms_occupational_mix_text_lines",
    "cms_occupational_mix_text_lines_utf16",
    "county_adjacency_2010_text_lines",
    "saipe_text_lines",
}
SHEET_TABLES = {"cms_ipps_sheet_rows", "cms_occupational_mix_sheet_rows", "hud_zip_county_sheet_rows", "rucc_sheet_rows", "ruca_sheet_rows"}


FIXTURE_COLUMNS = (
    "bronze_table",
    "_object_key",
    "_source_id",
    "_snapshot_id",
    "_dataset_id",
    "_release_id",
    "_s3_key",
    "_s3_version_id",
    "_member_path",
    "_member_sha256",
    "_row_number",
    "value",
    "line_text",
    "sheet_name",
    "cells_text",
    "facility_id",
    "provider_id",
    "state",
    "measure_id",
    "start_date",
    "end_date",
    "measure_start_date",
    "measure_end_date",
    "score",
    "footnote",
    "measure_name",
    "compared_to_national",
)
# Splits the fixture CSV (path in the fixture_csv variable) into the bronze tables; fixed text, never built from values.
FIXTURE_SQL = """CREATE SCHEMA bronze;
CREATE TABLE fixture AS
    SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('fixture_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_hai_hospital AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_hai_hospital';
CREATE TABLE bronze.cms_hai_state AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_hai_state';
CREATE TABLE bronze.cms_hai_national AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_hai_national';
CREATE TABLE bronze.cms_hospital_cost_reports AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_hospital_cost_reports_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_ipps_sas AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_ipps_sas';
CREATE TABLE bronze.cms_ipps_text_lines AS
    SELECT * EXCLUDE (bronze_table, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_ipps_text_lines';
CREATE TABLE bronze.cms_occupational_mix_text_lines AS
    SELECT * EXCLUDE (bronze_table, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_occupational_mix_text_lines';
CREATE TABLE bronze.cms_occupational_mix_text_lines_utf16 AS
    SELECT * EXCLUDE (bronze_table, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_occupational_mix_text_lines_utf16';
CREATE TABLE bronze.cms_ipps_sheet_rows AS
    SELECT * EXCLUDE (bronze_table, line_text, cells_text), _row_number AS sheet_row, string_split(cells_text, '|') AS cells
    FROM fixture WHERE bronze_table = 'cms_ipps_sheet_rows';
CREATE TABLE bronze.cms_occupational_mix_sheet_rows AS
    SELECT * EXCLUDE (bronze_table, line_text, cells_text), _row_number AS sheet_row, string_split(cells_text, '|') AS cells
    FROM fixture WHERE bronze_table = 'cms_occupational_mix_sheet_rows';
CREATE TABLE bronze.county_adjacency_2010_text_lines AS
    SELECT * EXCLUDE (bronze_table, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'county_adjacency_2010_text_lines';
CREATE TABLE bronze.saipe_text_lines AS
    SELECT * EXCLUDE (bronze_table, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'saipe_text_lines';
CREATE TABLE bronze.hud_zip_county_sheet_rows AS
    SELECT * EXCLUDE (bronze_table, line_text, cells_text), _row_number AS sheet_row, string_split(cells_text, '|') AS cells
    FROM fixture WHERE bronze_table = 'hud_zip_county_sheet_rows';
CREATE TABLE bronze.rucc_sheet_rows AS
    SELECT * EXCLUDE (bronze_table, line_text, cells_text), _row_number AS sheet_row, string_split(cells_text, '|') AS cells
    FROM fixture WHERE bronze_table = 'rucc_sheet_rows';
CREATE TABLE bronze.ruca_sheet_rows AS
    SELECT * EXCLUDE (bronze_table, line_text, cells_text), _row_number AS sheet_row, string_split(cells_text, '|') AS cells
    FROM fixture WHERE bronze_table = 'ruca_sheet_rows';
DROP TABLE fixture;
CREATE TABLE bronze.cms_provider_of_services AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_provider_of_services_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_cc_hospital_general_information AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_cc_hospital_general_information_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_hospital_enrollments AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_hospital_enrollments_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_hospital_owners AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_hospital_owners_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_change_of_ownership AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_change_of_ownership_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_cc_timely_and_effective_care_hospital AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_cc_timely_and_effective_care_hospital_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_cc_unplanned_hospital_visits_hospital AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_cc_unplanned_hospital_visits_hospital_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_cc_complications_and_deaths_hospital AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_cc_complications_and_deaths_hospital_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_cc_hospital_readmissions_reduction_program_hospital AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(
        getvariable('cms_cc_hospital_readmissions_reduction_program_hospital_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"'
    );
CREATE TABLE bronze.cms_cc_hac_reduction_program_hospital AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_cc_hac_reduction_program_hospital_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_cc_hvbp_tps AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_cc_hvbp_tps_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_cc_maternal_health_hospital AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_cc_maternal_health_hospital_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_cc_hcahps_hospital AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_cc_hcahps_hospital_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_medicare_inpatient_by_provider AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_medicare_inpatient_by_provider_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_medicare_inpatient_by_drg AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_medicare_inpatient_by_drg_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.hhs_capacity_csv AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('hhs_capacity_csv_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.onc_pi_chpl_linkage_csv AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('onc_pi_chpl_linkage_csv_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.onc_pi_attestations_csv AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('onc_pi_attestations_csv_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.hud_zip_county AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('hud_zip_county_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.county_adjacency AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('county_adjacency_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.rucc AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('rucc_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.ruca_tracts_2020 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('ruca_tracts_2020_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.ruca_zip_2020 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('ruca_zip_2020_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.ruca_zip_2010 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('ruca_zip_2010_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_hsa_csv AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_hsa_csv_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.svi AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('svi_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_dp02 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_dp02_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_dp03 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_dp03_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_dp04 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_dp04_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_dp05 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_dp05_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_s0101 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_s0101_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_s0601 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_s0601_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_s1701 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_s1701_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_s2503 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_s2503_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_s2701 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_s2701_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_b16005 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_b16005_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_b19013 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_b19013_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_b25070 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_b25070_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_b25091 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_b25091_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_b26001 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_b26001_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_c16001 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_c16001_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_summary_b16005 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_summary_b16005_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_summary_b19013 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_summary_b19013_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_summary_b25070 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_summary_b25070_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_summary_b25091 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_summary_b25091_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_summary_b26001 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_summary_b26001_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.acs_summary_c16001 AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('acs_summary_c16001_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.sahie AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('sahie_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.bls_laus AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('bls_laus_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.places AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('places_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_geographic_variation_csv AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_geographic_variation_csv_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.wonder_county_mortality AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('wonder_county_mortality_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_mmd_csv AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('cms_mmd_csv_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.hrsa_hpsa_detail AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('hrsa_hpsa_detail_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.hrsa_mua_detail AS SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('hrsa_mua_detail_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.file_preambles AS SELECT * REPLACE (line_number::INTEGER AS line_number)
    FROM read_csv(getvariable('file_preambles_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.column_map AS SELECT * REPLACE (position::INTEGER AS position)
    FROM read_csv(getvariable('column_map_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.stored_copies AS
    SELECT * REPLACE (byte_count::BIGINT AS byte_count, loaded::BOOLEAN AS loaded, retired::BOOLEAN AS retired)
    FROM read_csv(getvariable('copies_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
"""
COPY_COLUMNS = (
    "table_name",
    "_object_key",
    "sha256",
    "s3_key",
    "s3_version_id",
    "source_id",
    "snapshot_id",
    "dataset_id",
    "release_id",
    "release_partition",
    "manifest_key",
    "manifest_version_id",
    "member_path",
    "file_name",
    "byte_count",
    "loaded",
    "retired",
)


def loaded_keys(objects: Iterable[Stored]) -> set[str]:
    """Return the copies bronze loads: the smallest object key of each table's file [205]."""
    canonical: dict[tuple[str, str], str] = {}
    for item in objects:
        file = (item.table, item.sha)
        canonical[file] = min(canonical.get(file, item.key), item.key)
    return set(canonical.values())


def copies_csv(objects: Iterable[Stored]) -> str:
    """Return the copies table as CSV: every stored copy with its lineage and whether bronze loads it [206]."""
    items = list(objects)
    loaded = loaded_keys(items)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(COPY_COLUMNS)
    for item in items:
        file_name = item.member.rsplit("!", 1)[-1].rsplit("/", 1)[-1]
        writer.writerow(
            (item.table, item.key, item.sha, f"fixture/{item.key}.zip", f"v-{item.key}", "fixture", item.snapshot, "fixture_dataset", item.release)
            + ("", "fixture/manifest.json", "v-manifest", item.member, file_name, item.rows * 10, str(item.key in loaded).lower(), "false")
        )
    return buffer.getvalue()


def fixture_csv(objects: Iterable[Stored]) -> str:
    """Return the fixture rows as CSV: the provenance columns, one data column and the text or sheet content."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(FIXTURE_COLUMNS)
    items = list(objects)
    loaded = loaded_keys(items)
    for item in items:
        if (item.key not in loaded and not item.force_loaded) or item.table in WIDE_COLUMNS:
            continue
        if item.content and len(item.content) != item.rows:
            raise ValueError(f"fixture {item.key}: {len(item.content)} content rows but rows={item.rows}")
        for row in range(1, item.rows + 1):
            # The same bytes give the same rows; the second checksum, when set, marks the object's last row.
            checksum = item.second_sha if item.second_sha and row == item.rows else item.sha
            value = f"{item.sha[:6]}-{row}"
            text = item.content[row - 1] if item.content else value
            sheet, _, cells = text.partition(":") if item.table in SHEET_TABLES and item.content else ("Sheet1", "", value)
            # HAI content fills the HAI columns under the newer layout's names; empty parts stay null. A sixth field is the
            # footnote, a seventh the national comparison [555] [563].
            hai = {"facility_id": "", "state": "", "measure_id": "", "start_date": "", "end_date": "", "score": "", "footnote": "", "compared": ""}
            if item.table in HAI_TABLES and item.content:
                entity, measure, start, end, score, *rest = text.split("|")
                entity_column = HAI_TABLES[item.table]
                if entity_column:
                    hai[entity_column] = entity
                rest = [*rest, "", ""]
                hai.update(measure_id=measure, start_date=start, end_date=end, score=score, footnote=rest[0], compared=rest[1])
            writer.writerow(
                (item.table, item.key, "fixture", item.snapshot, "fixture_dataset", item.release, f"fixture/{item.key}.zip", f"v-{item.key}", item.member)
                + (checksum, row, value, text, sheet, cells)
                + (
                    hai["facility_id"],
                    "",
                    hai["state"],
                    hai["measure_id"],
                    hai["start_date"],
                    hai["end_date"],
                    "",
                    "",
                    hai["score"],
                    hai["footnote"],
                    "",
                    hai["compared"],
                )
            )
    return buffer.getvalue()


def wide_csv(objects: Iterable[Stored], table: str) -> str:
    """Return one wide table's rows as CSV: the provenance columns and the table's bronze columns, absent ones empty."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(PROVENANCE_COLUMNS + WIDE_COLUMNS[table])
    items = list(objects)
    loaded = loaded_keys(items)
    for item in items:
        if item.table != table or (item.key not in loaded and not item.force_loaded):
            continue
        if len(item.records) != item.rows:
            raise ValueError(f"fixture {item.key}: {len(item.records)} records but rows={item.rows}")
        for row, record in enumerate(item.records, start=1):
            unknown = set(dict(record)) - set(WIDE_COLUMNS[table])
            if unknown:
                raise ValueError(f"fixture {item.key}: columns {sorted(unknown)} are not in the {table} fixture")
            values = dict(record)
            provenance = (item.key, "fixture", item.snapshot, "fixture_dataset", item.release, f"fixture/{item.key}.zip", f"v-{item.key}", item.member)
            # As in fixture_csv: the second checksum, when set, marks the object's last row.
            checksum = item.second_sha if item.second_sha and row == item.rows else item.sha
            writer.writerow(provenance + (checksum, row) + tuple(values.get(column, "") for column in WIDE_COLUMNS[table]))
    return buffer.getvalue()


def periods_csv(objects: Iterable[Stored], unlabelled: frozenset[str]) -> str:
    """Return the POS period seed for the fixture's POS files, without the ones a case leaves out [281]."""
    rows = [
        {"member_sha256": item.sha, "release_id": item.release, "file_name": item.member, "period_start": start, "period_end": end}
        for item in objects
        if item.table == "cms_provider_of_services" and item.key not in unlabelled
        for start, end in (POS_PERIODS[item.key],)
    ]
    return pos_file_periods.as_csv(rows)


def ownership_periods_csv(objects: Iterable[Stored], unlabelled: frozenset[str]) -> str:
    """Return the ownership period seed for the fixture's files, written by the real generator, without the ones a case leaves out [365]."""
    owned = [item for item in objects if item.table in ownership_release_periods.TABLES and item.key not in unlabelled]
    loaded = [{"table": item.table, "sha256": item.sha, "release_id": item.release, "file_name": item.member} for item in owned]
    periods = {item.release: OWNERSHIP_PERIODS[item.key] for item in owned}
    return ownership_release_periods.as_csv(ownership_release_periods.rows_for(loaded, periods))


def column_map_csv(objects: Iterable[Stored]) -> str:
    """Return bronze.column_map for the fixture: each loaded object's published labels by header [420]."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(("_object_key", "table_name", "position", "original_header", "column_name", "original_label"))
    items = list(objects)
    loaded = loaded_keys(items)
    for item in items:
        if item.key in loaded or item.force_loaded:
            for position, (header, label) in enumerate(item.labels, start=1):
                writer.writerow((item.key, item.table, position, header, header.lower(), label))
    return buffer.getvalue()


def file_preambles_csv(objects: Iterable[Stored]) -> str:
    """Return bronze.file_preambles for the fixture: each loaded object's note lines before its header [439]."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(("_object_key", "table_name", "line_number", "line_text"))
    items = list(objects)
    loaded = loaded_keys(items)
    for item in items:
        if item.key in loaded or item.force_loaded:
            for number, line in enumerate(item.preamble, start=1):
                writer.writerow((item.key, item.table, number, line))
    return buffer.getvalue()


def acs_map_csv(rows: Iterable[dict[str, str]]) -> str:
    """Return the fixture's ACS variable map in the reviewed seed's columns [420] [423]."""
    columns = ("concept_id", "bronze_table", "vintage", "column_name", "published_label", "status", "reason")
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def geography_periods_csv(objects: Iterable[Stored], unlabelled: frozenset[str]) -> str:
    """Return the geography period seed for the fixture's files, written by the real generator, without the ones a case leaves out [403]."""
    loaded = [
        {"table": item.table, "sha256": item.sha, "release_id": item.release, "file_name": item.member}
        for item in objects
        if item.table in geography_file_periods.TABLES and item.key not in unlabelled
    ]
    return geography_file_periods.as_csv(geography_file_periods.rows_for(loaded, GEOGRAPHY_QUARTERS, GEOGRAPHY_COVERAGE, SVI_EDITIONS, WONDER_DATABASES))


def publication(item: Stored) -> tuple[str, str]:
    """Return the expected publication release and its source [170]."""
    if "!" not in item.member:
        return item.release, "release_partition"
    nested = NESTED_DATE.match(item.member)
    return (nested.group(1), "nested_archive") if nested else (item.release, "container")


def expected(objects: Iterable[Stored]) -> dict[str, Any]:
    """Compute the copy and file models' expected content from the fixture, independently of the dbt SQL."""
    items = list(objects)
    canonical: dict[tuple[str, str], str] = {}
    for item in items:
        file = (item.table, item.sha)
        canonical[file] = min(canonical.get(file, item.key), item.key)
    # Only the loaded copy has rows in bronze; the other copies are listed without a row count [209].
    copies = sorted(
        (item.table, item.key, item.sha, *publication(item), str(canonical[(item.table, item.sha)] == item.key).lower())
        + (str(item.rows) if canonical[(item.table, item.sha)] == item.key else "",)
        for item in items
    )
    files: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        entry = files.setdefault((item.table, item.sha), {"copies": 0, "rows": item.rows, "releases": set()})
        entry["copies"] += 1
        entry["releases"].add(publication(item)[0])
    file_rows = sorted(
        (table, digest, canonical[(table, digest)], str(entry["copies"]), str(entry["rows"]), "|".join(sorted(entry["releases"])), HELD.get(digest, ""))
        for (table, digest), entry in files.items()
    )
    rows = {table: sum(entry["rows"] for (name, _), entry in files.items() if name == table) for table in TABLES}
    return {"copies": copies, "files": file_rows, "rows": rows}


COPIES_SQL = (
    "SELECT bronze_table, object_key, member_sha256, publication_release, release_source, is_canonical::VARCHAR, coalesce(row_count::VARCHAR, '') "
    "FROM stg_bronze__file_copies ORDER BY ALL;"
)
FILES_SQL = (
    "SELECT bronze_table, member_sha256, canonical_object_key, copy_count::VARCHAR, row_count::VARCHAR, "
    "array_to_string(publication_releases, '|'), coalesce(label_hold_issue, '') FROM stg_bronze__files ORDER BY ALL;"
)


WINDOWS_SQL = (
    "SELECT level, entity_id, measure_id, window_start::VARCHAR, window_end::VARCHAR, score, left(member_sha256, 2) FROM ("
    "SELECT 'hospital' AS level, * FROM int_hai_hospital_windows UNION ALL BY NAME "
    "SELECT 'state' AS level, * FROM int_hai_state_windows UNION ALL BY NAME "
    "SELECT 'national' AS level, * FROM int_hai_national_windows) ORDER BY ALL;"
)
HOLDS_SQL = "SELECT bronze_table, coalesce(entity_id, ''), coalesce(measure_id, ''), hold_reason, row_count::VARCHAR FROM int_hai_window_holds ORDER BY ALL;"
TWINS_SQL = (
    "SELECT left(text_sha256, 2), left(workbook_sha256, 2), text_layout, twin_status, coalesce(left(preferred_sha256, 2), '') "
    "FROM stg_bronze__twin_comparison ORDER BY ALL;"
)
SELECTION_SQL = (
    "SELECT left(member_sha256, 2), has_label_conflict::VARCHAR, is_label_held::VARCHAR, is_twin_excluded::VARCHAR, is_selected::VARCHAR "
    "FROM stg_bronze__file_selection WHERE NOT is_selected ORDER BY ALL;"
)
# Sheets of twin-excluded workbooks; every sheet of a selected workbook is file_selected [273].
SHEETS_SQL = (
    "SELECT left(member_sha256, 2), sheet_name, sheet_status, coalesce(left(covering_sha256, 2), ''), is_selected::VARCHAR "
    "FROM stg_bronze__sheet_selection WHERE sheet_status <> 'file_selected' ORDER BY ALL;"
)
POS_SQL = (
    "SELECT ccn, period_end::VARCHAR, coalesce(state_code, ''), coalesce(county_fips, ''), coalesce(ssa_county_code, ''), "
    "coalesce(provider_subtype_code, ''), coalesce(control_type_code, ''), coalesce(termination_code, ''), coalesce(is_active::VARCHAR, ''), "
    "coalesce(bed_count::VARCHAR, ''), coalesce(certified_bed_count::VARCHAR, ''), coalesce(rn_count::VARCHAR, ''), "
    "coalesce(has_residency_allopathic::VARCHAR, ''), coalesce(has_residency_dental::VARCHAR, ''), coalesce(icu_service_code, ''), "
    "coalesce(original_participation_date::VARCHAR, ''), coalesce(termination_date::VARCHAR, ''), coalesce(zip_code, ''), "
    "is_state_or_dc::VARCHAR, is_connecticut::VARCHAR, is_critical_access_by_ccn::VARCHAR, is_veterans_affairs::VARCHAR "
    "FROM int_pos_hospital_snapshots ORDER BY ALL;"
)
CMI_ROWS_SQL = (
    "SELECT left(member_sha256, 2), rule_fiscal_year::VARCHAR, coalesce(data_fiscal_year::VARCHAR, ''), rule_stage, ccn, cmi::VARCHAR, "
    "coalesce(cases::VARCHAR, ''), coalesce(relative_weights::VARCHAR, ''), coalesce(transfer_adjusted_cmi::VARCHAR, ''), has_header::VARCHAR "
    "FROM int_cmi_hospital_rows ORDER BY ALL;"
)
CMI_YEARS_SQL = (
    "SELECT ccn, rule_fiscal_year::VARCHAR, coalesce(data_fiscal_year::VARCHAR, ''), rule_stage, cmi::VARCHAR, left(member_sha256, 2) "
    "FROM int_cmi_hospital_years ORDER BY ALL;"
)
CMI_HOLDS_SQL = "SELECT year_basis, ccn, fiscal_year::VARCHAR, hold_reason, file_count::VARCHAR FROM int_cmi_holds ORDER BY ALL;"
CMI_DATA_YEARS_SQL = (
    "SELECT ccn, data_fiscal_year::VARCHAR, rule_fiscal_year::VARCHAR, rule_stage, cmi::VARCHAR, left(member_sha256, 2) "
    "FROM int_cmi_hospital_data_years ORDER BY ALL;"
)
# Silver step 7.3: SCD2 history, versions dated by source dates only [670] to [677].
POS_HISTORY_SQL = (
    "SELECT ccn, valid_from::VARCHAR, coalesce(valid_to::VARCHAR, ''), is_current::VARCHAR, is_after_gap::VARCHAR, "
    "release_count::VARCHAR, coalesce(control_type_code, '') FROM int_hospital_pos_history ORDER BY ALL;"
)
OWNER_HISTORY_SQL = (
    "SELECT enrollment_id, associate_id_owner, role_code, valid_from::VARCHAR, coalesce(valid_to::VARCHAR, ''), is_current::VARCHAR, "
    "is_after_gap::VARCHAR, release_count::VARCHAR FROM int_hospital_ownership_history ORDER BY ALL;"
)
HGI_HISTORY_SQL = "SELECT count(*)::VARCHAR, count(DISTINCT ccn)::VARCHAR, count(*) FILTER (WHERE is_current)::VARCHAR FROM int_hospital_hgi_history;"
SPINE_SQL = (
    "SELECT ccn, window_year::VARCHAR, coalesce(pos_period_end::VARCHAR, ''), coalesce(state_code, ''), coalesce(county_fips, ''), "
    "coalesce(cmi::VARCHAR, ''), coalesce(cmi_data_fiscal_year::VARCHAR, ''), coalesce(cmi_rule_fiscal_year::VARCHAR, ''), "
    "coalesce(cmi_rule_stage, ''), has_pos_snapshot::VARCHAR, has_cmi::VARCHAR, is_cmi_held::VARCHAR, is_critical_access::VARCHAR, "
    "is_veterans_affairs::VARCHAR, is_state_or_dc::VARCHAR, is_connecticut::VARCHAR, is_primary_population::VARCHAR, "
    "is_sensitivity_population::VARCHAR, coalesce(state_source, ''), coalesce(county_source, ''), "
    "coalesce(classification_source, ''), coalesce(provider_subtype_code, ''), coalesce(planning_region_fips, ''), "
    "coalesce(planning_region_source, '') FROM int_hospital_spine ORDER BY ALL;"
)
TE_SQL = (
    "SELECT entity_id, measure_id, window_start::VARCHAR, window_end::VARCHAR, coalesce(score, ''), coalesce(sample, ''), left(member_sha256, 2) "
    "FROM int_cc_timely_effective_windows ORDER BY ALL;"
)
MATERNAL_SQL = (
    "SELECT entity_id, measure_id, window_start::VARCHAR, window_end::VARCHAR, coalesce(score, ''), coalesce(sample, ''), left(member_sha256, 2) "
    "FROM int_cc_maternal_windows ORDER BY ALL;"
)
HCAHPS_SQL = (
    "SELECT entity_id, measure_id, window_start::VARCHAR, window_end::VARCHAR, coalesce(hcahps_answer_percent, ''), "
    "coalesce(number_of_completed_surveys, ''), coalesce(survey_response_rate_percent, ''), coalesce(patient_survey_star_rating, ''), "
    "left(member_sha256, 2) FROM int_cc_hcahps_windows ORDER BY ALL;"
)
CC_HOLDS_SQL = "SELECT bronze_table, coalesce(entity_id, ''), coalesce(measure_id, ''), hold_reason, row_count::VARCHAR FROM int_cc_window_holds ORDER BY ALL;"
REGISTRY_SQL = (
    "SELECT measure_control, entity_id, window_start::VARCHAR, coalesce(value_text, ''), coalesce(value_number::VARCHAR, ''), "
    "coalesce(footnote_text, '') FROM int_registry_measure_windows ORDER BY ALL;"
)
HGI_SQL = (
    "SELECT ccn, release_date::VARCHAR, release_file_count::VARCHAR, coalesce(hospital_type, ''), coalesce(has_emergency_services::VARCHAR, ''), "
    "coalesce(overall_rating::VARCHAR, ''), coalesce(overall_rating_text, ''), coalesce(overall_rating_footnote, ''), left(member_sha256, 2) "
    "FROM int_hgi_hospital_releases ORDER BY ALL;"
)
COST_REPORTS_SQL = (
    "SELECT rpt_rec_num, ccn, fiscal_year::VARCHAR, file_fiscal_year::VARCHAR, period_begin::VARCHAR, period_end::VARCHAR, "
    "reporting_days::VARCHAR, is_full_year::VARCHAR, reports_in_fiscal_year::VARCHAR, coalesce(zip_code, ''), coalesce(rural_urban, ''), "
    "coalesce(number_of_beds::VARCHAR, ''), coalesce(cost_to_charge_ratio::VARCHAR, ''), coalesce(total_bad_debt_expense::VARCHAR, '') "
    "FROM int_cost_reports ORDER BY ALL;"
)
COST_MEASURES_SQL = (
    "SELECT rpt_rec_num, measure_control, coalesce(value_number::VARCHAR, ''), coalesce(value_code, '') FROM int_cost_report_measures "
    "WHERE rpt_rec_num IN ('100001', '100002') ORDER BY ALL;"
)
IMPACT_VALUES_SQL = (
    "SELECT rule_fiscal_year::VARCHAR, rule_stage, sheet_name, ccn, field, coalesce(value_text, ''), coalesce(value_number::VARCHAR, '') "
    "FROM int_impact_hospital_values ORDER BY ALL;"
)
IMPACT_HOLDS_SQL = "SELECT rule_fiscal_year::VARCHAR, sheet_name, ccn, hold_reason, hold_rows::VARCHAR FROM int_impact_holds ORDER BY ALL;"
IMPACT_MEASURES_SQL = (
    "SELECT ccn, measure_control, field, coalesce(value_number::VARCHAR, ''), coalesce(value_code, '') FROM int_impact_measures "
    "WHERE rule_fiscal_year = 2026 ORDER BY ALL;"
)
MUP_PROVIDERS_SQL = (
    "SELECT data_year::VARCHAR, release_year::VARCHAR, ccn, coalesce(tot_benes::VARCHAR, ''), coalesce(tot_dschrgs::VARCHAR, ''), "
    "coalesce(bene_race_natind_cnt::VARCHAR, ''), coalesce(bene_cc_ph_ckd_v2_pct::VARCHAR, '') FROM int_mup_providers ORDER BY ALL;"
)
MUP_DRG_SQL = "SELECT data_year::VARCHAR, ccn, drg_cd, tot_dschrgs::VARCHAR FROM int_mup_drg_discharges ORDER BY ALL;"
MUP_MEASURES_SQL = "SELECT ccn, measure_control, field, coalesce(value_number::VARCHAR, '') FROM int_mup_measures WHERE data_year = 2023 ORDER BY ALL;"
HHS_SQL = (
    "SELECT left(hospital_pk, 6), collection_week::VARCHAR, coalesce(ccn, ''), coalesce(is_corrected::VARCHAR, ''), "
    "coalesce(total_beds_7_day_avg::VARCHAR, ''), coalesce(inpatient_beds_used_covid_7_day_avg::VARCHAR, ''), "
    "coalesce(inpatient_beds_used_7_day_avg::VARCHAR, ''), coalesce(total_beds_7_day_coverage::VARCHAR, ''), "
    "array_to_string(suppressed_fields, '|'), array_to_string(negative_fields, '|') FROM int_hhs_capacity_weeks ORDER BY ALL;"
)
HHS_SOURCES_SQL = (
    "SELECT left(hospital_pk, 6), coalesce(ccn, ''), coalesce(ccn_source, '') FROM int_hhs_capacity_weeks "
    "WHERE coalesce(ccn_source, '') <> 'published' ORDER BY ALL;"
)
ONC_CHPL_SQL = (
    "SELECT ccn, coalesce(meets_criteria_for_promoting_interoperability_of_ehrs::VARCHAR, ''), coalesce(start_date::VARCHAR, ''), "
    "coalesce(end_date::VARCHAR, ''), coalesce(program_year::VARCHAR, ''), coalesce(chpl_id, ''), coalesce(developer_name, '') "
    "FROM int_onc_chpl_linkage_rows ORDER BY ALL;"
)
ONC_ATTESTATIONS_SQL = (
    "SELECT ccn, coalesce(program_year::VARCHAR, ''), coalesce(attestation_month::VARCHAR, ''), coalesce(attestation_year::VARCHAR, ''), "
    "coalesce(vendor_name, '') FROM int_onc_attestation_rows ORDER BY ALL;"
)
PHONE_COLUMNS_SQL = (
    "SELECT count(*)::VARCHAR FROM information_schema.columns WHERE table_name = 'int_onc_chpl_linkage_rows' AND column_name LIKE '%telephone%';"
)
OWNERS_SQL = (
    "SELECT left(member_sha256, 2), period_end::VARCHAR, enrollment_id, coalesce(associate_id_owner, ''), coalesce(role_code, ''), "
    "coalesce(association_date::VARCHAR, ''), coalesce(percentage_ownership::VARCHAR, ''), flags_published::VARCHAR, "
    "coalesce(private_equity_company_owner::VARCHAR, ''), coalesce(reit_owner::VARCHAR, ''), coalesce(owned_by_another_org_or_ind_owner::VARCHAR, ''), "
    "coalesce(for_profit_owner::VARCHAR, '') FROM int_hospital_owner_rows ORDER BY ALL;"
)
ENROLLMENT_SOURCES_SQL = (
    "SELECT enrollment_id, coalesce(ccn, ''), coalesce(ccn_source, '') FROM int_hospital_enrollment_rows "
    "WHERE coalesce(ccn_source, '') <> 'published' ORDER BY ALL;"
)
ENROLLMENTS_SQL = (
    "SELECT left(member_sha256, 2), period_end::VARCHAR, enrollment_id, coalesce(ccn, ''), coalesce(ccn_published, ''), "
    "coalesce(proprietary_nonprofit, ''), coalesce(incorporation_date::VARCHAR, ''), coalesce(subgroup_acute_care::VARCHAR, ''), "
    "coalesce(reh_conversion_flag::VARCHAR, '') FROM int_hospital_enrollment_rows ORDER BY ALL;"
)
CHOW_SQL = (
    "SELECT left(member_sha256, 2), period_end::VARCHAR, coalesce(ccn_buyer, ''), coalesce(ccn_buyer_published, ''), coalesce(ccn_seller, ''), "
    "coalesce(chow_type_code, ''), coalesce(effective_date::VARCHAR, ''), event_key FROM int_change_of_ownership_rows ORDER BY ALL;"
)
VIEW_ROWS_SQL = "SELECT getvariable('checked_table'), count(*)::VARCHAR FROM query_table(getvariable('checked_table'));\n"
BRONZE_COUNTS_SQL = (
    "WITH o AS (SELECT _object_key, any_value(_member_sha256) AS sha, count(*) AS n FROM query_table(getvariable('checked_table')) GROUP BY 1), "
    "d AS (SELECT DISTINCT sha, n FROM o) "
    "SELECT getvariable('checked_table'), (SELECT count(*) FROM o)::VARCHAR, (SELECT count(*) FROM d)::VARCHAR, (SELECT sum(n) FROM d)::VARCHAR;\n"
)


OCCMIX_SQL = (
    "SELECT ccn_published, survey_start_date::VARCHAR, survey_end_date::VARCHAR, is_deleted::VARCHAR, "
    "coalesce(rnhr::VARCHAR, ''), coalesce(rn_paid_hour_wage::VARCHAR, ''), "
    "coalesce(round(rn_paid_hour_share, 4)::VARCHAR, ''), "
    "coalesce(round(lpnst_paid_hour_share, 4)::VARCHAR, ''), coalesce(round(naorat_paid_hour_share, 4)::VARCHAR, '') "
    "FROM int_occmix_survey_rows ORDER BY ALL;"
)

HUD_SQL = (
    "SELECT quarter_label, zip_code, county_fips, county_scope, is_connecticut::VARCHAR, res_ratio::VARCHAR, tot_ratio::VARCHAR, "
    "has_residential_addresses::VARCHAR FROM int_hud_zip_county_quarters ORDER BY ALL;"
)
HUD_HOLDS_SQL = "SELECT quarter_label, zip_code, geoid_published, hold_reason FROM int_hud_zip_county_holds ORDER BY ALL;"
ADJACENCY_SQL = (
    "SELECT vintage, county_fips, coalesce(county_name, ''), coalesce(neighbor_fips, ''), coalesce(shared_border_length_m::VARCHAR, ''), "
    "is_self_link::VARCHAR, is_isolated::VARCHAR, is_connecticut::VARCHAR FROM int_county_adjacency_edges ORDER BY ALL;"
)
RUCC_SQL = (
    "SELECT vintage, county_fips, coalesce(rucc_code, ''), coalesce(rucc_published, ''), coalesce(population::VARCHAR, ''), "
    "is_connecticut::VARCHAR FROM int_rucc_county_codes ORDER BY ALL;"
)
RUCA_SQL = (
    "SELECT vintage, geography_type, geography_id, coalesce(county_fips, ''), coalesce(primary_ruca, ''), coalesce(secondary_ruca, ''), "
    "coalesce(primary_published, ''), coalesce(secondary_published, '') FROM int_ruca_codes ORDER BY ALL;"
)
HSA_SQL = (
    "SELECT data_year, ccn_published, is_ccn_shape_valid::VARCHAR, coalesce(zip_code, ''), is_zip_suppressed::VARCHAR, is_zip_missing::VARCHAR, "
    "coalesce(total_cases::VARCHAR, ''), is_cases_suppressed::VARCHAR, coalesce(total_charges::VARCHAR, ''), is_charges_suppressed::VARCHAR "
    "FROM int_hsa_zip_cases ORDER BY ALL;"
)

ACS_SQL = (
    "SELECT vintage, concept_id, column_name, county_fips, coalesce(value_number::VARCHAR, ''), coalesce(missing_token, ''), is_top_coded::VARCHAR "
    "FROM int_acs_county_values ORDER BY ALL;"
)
SVI_SQL = "SELECT edition, county_fips, field, coalesce(value_number::VARCHAR, ''), coalesce(missing_token, '') FROM int_svi_county_values ORDER BY ALL;"

SAIPE_SQL = (
    "SELECT estimate_year::VARCHAR, county_fips, coalesce(poverty_all_count::VARCHAR, ''), coalesce(poverty_all_pct::VARCHAR, ''), "
    "coalesce(median_household_income::VARCHAR, ''), coalesce(median_household_income_lb90::VARCHAR, ''), array_to_string(missing_fields, '|') "
    "FROM int_saipe_county_estimates ORDER BY ALL;"
)
SAHIE_SQL = (
    "SELECT estimate_year::VARCHAR, county_fips, agecat, racecat, sexcat, iprcat, is_all_groups::VARCHAR, coalesce(nipr::VARCHAR, ''), "
    "coalesce(nui::VARCHAR, ''), coalesce(pctui::VARCHAR, ''), coalesce(pctui_moe::VARCHAR, ''), array_to_string(missing_fields, '|') "
    "FROM int_sahie_county_rows ORDER BY ALL;"
)
BLS_SQL = (
    "SELECT county_fips, measure, data_year::VARCHAR, coalesce(month_number::VARCHAR, ''), is_annual_average::VARCHAR, is_seasonally_adjusted::VARCHAR, "
    "coalesce(value_number::VARCHAR, ''), coalesce(missing_token, ''), array_to_string(footnote_codes, '|'), capture_date "
    "FROM int_bls_county_series ORDER BY ALL;"
)

PLACES_SQL = (
    "SELECT edition, data_year::VARCHAR, measureid, datavaluetypeid, count(*)::VARCHAR, count(data_value)::VARCHAR, bool_and(is_all_states)::VARCHAR "
    "FROM int_places_county_values GROUP BY ALL ORDER BY ALL;"
)
PLACES_DETAIL_SQL = (
    "SELECT county_fips, measureid, datavaluetypeid, coalesce(data_value::VARCHAR, ''), coalesce(low_confidence_limit::VARCHAR, ''), "
    "coalesce(footnote_symbol, ''), is_connecticut::VARCHAR FROM int_places_county_values WHERE measureid <> 'OBESITY' ORDER BY ALL;"
)
GV_SQL = (
    "SELECT data_year::VARCHAR, county_fips, field, coalesce(value_number::VARCHAR, ''), coalesce(missing_token, '') FROM int_gv_county_values ORDER BY ALL;"
)
WONDER_SQL = (
    "SELECT wonder_database, county_fips, data_year::VARCHAR, coalesce(deaths::VARCHAR, ''), coalesce(crude_rate::VARCHAR, ''), "
    "coalesce(crude_rate_token, ''), "
    "coalesce(hold_reason, '') FROM int_wonder_county_deaths ORDER BY ALL;"
)
D1_WINDOWS_SQL = (
    "SELECT 'visits', entity_id, measure_id, window_start::VARCHAR, coalesce(score, ''), left(member_sha256, 3) FROM int_cc_unplanned_visits_windows "
    "UNION ALL SELECT 'deaths', entity_id, measure_id, window_start::VARCHAR, coalesce(score, ''), left(member_sha256, 3) "
    "FROM int_cc_complications_deaths_windows "
    "UNION ALL SELECT 'hrrp', entity_id, measure_id, window_start::VARCHAR, coalesce(excess_readmission_ratio, ''), left(member_sha256, 3) "
    "FROM int_cc_hrrp_windows ORDER BY ALL;"
)
VALIDATION_SQL = (
    "SELECT measure_control, entity_id, measure_id, window_start::VARCHAR, coalesce(value_text, ''), coalesce(value_number::VARCHAR, '') "
    "FROM int_validation_measure_windows ORDER BY ALL;"
)
VALIDATION_ALIASES_SQL = (
    "SELECT DISTINCT entity_id, measure_id, published_measure_id FROM int_validation_measure_windows WHERE measure_id <> published_measure_id ORDER BY ALL;"
)
VALIDATION_HOLDS_SQL = (
    "SELECT bronze_table, coalesce(entity_id, ''), coalesce(measure_id, ''), hold_reason, row_count::VARCHAR FROM int_validation_window_holds ORDER BY ALL;"
)
OUTCOME_SQL = (
    "SELECT ccn, window_year::VARCHAR, hai_type, coalesce(sir_text, ''), coalesce(sir::VARCHAR, ''), coalesce(ci_lower::VARCHAR, ''), "
    "coalesce(ci_upper::VARCHAR, ''), coalesce(observed::VARCHAR, ''), coalesce(predicted::VARCHAR, ''), coalesce(exposure::VARCHAR, ''), "
    "coalesce(sir_footnote_codes, ''), baseline_held_parts::VARCHAR, is_primary_population::VARCHAR FROM int_spine_hai_outcomes "
    "WHERE published_parts > 0 OR baseline_held_parts > 0 ORDER BY ALL;"
)
OUTCOME_STATUS_SQL = (
    "SELECT ccn, window_year::VARCHAR, hai_type, alignment_status, staging_held_parts::VARCHAR, coalesce(sir_compared_to_national, ''), "
    "coalesce(observed_footnote, ''), coalesce(ci_lower_footnote, '') FROM int_spine_hai_outcomes "
    "WHERE alignment_status <> 'aligned' AND alignment_status <> 'not_in_source' OR sir_compared_to_national IS NOT NULL "
    "OR observed_footnote IS NOT NULL OR ci_lower_footnote IS NOT NULL ORDER BY ALL;"
)
OUTCOME_STATUS_COUNT_SQL = "SELECT alignment_status, count(*)::VARCHAR FROM int_spine_hai_outcomes GROUP BY 1 ORDER BY 1;"
OUTCOME_VALUES_SQL = "SELECT measure_control, count(*)::VARCHAR FROM int_hai_outcome_values GROUP BY 1 ORDER BY 1;"
OUTCOME_VALUE_ROWS_SQL = (
    "SELECT measure_control, outcome_key, field, value_text, coalesce(value_number::VARCHAR, ''), coalesce(review_decision, '') "
    "FROM int_hai_outcome_values WHERE outcome_key = '070001:2021:HAI_2' OR outcome_key = '070001:2021:HAI_3' ORDER BY ALL;"
)
VALIDATION_ALIGNED_SQL = (
    "SELECT ccn, window_year::VARCHAR, measure_control, measure, alignment_status, coalesce(value_text, ''), coalesce(value_number::VARCHAR, ''), "
    "coalesce(period_start::VARCHAR, ''), coalesce(period_end::VARCHAR, ''), coalesce(fiscal_year::VARCHAR, ''), coalesce(overlap_days::VARCHAR, '') "
    "FROM int_spine_validation_measures WHERE alignment_status IN ('aligned', 'held_in_staging', 'no_matching_period') ORDER BY ALL;"
)
VALIDATION_ALIGNED_COUNT_SQL = (
    "SELECT 'rows', (SELECT count(*) FROM int_spine_validation_measures)::VARCHAR UNION ALL "
    "SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_validation_measures)::VARCHAR UNION ALL "
    "SELECT alignment_status, count(*)::VARCHAR FROM int_spine_validation_measures GROUP BY 1 ORDER BY 1;"
)
COUNTY_SQL = (
    "SELECT ccn, window_year::VARCHAR, measure_source, measure_control, field, alignment_status, coalesce(value_text, ''), "
    "coalesce(value_number::VARCHAR, ''), coalesce(period_end::VARCHAR, ''), coalesce(age_months::VARCHAR, ''), is_primary_county_join::VARCHAR "
    "FROM int_spine_county_measures WHERE alignment_status IN ('aligned', 'held_in_staging') ORDER BY ALL;"
)
AL4B_SQL = (
    "SELECT ccn, window_year::VARCHAR, measure_source, measure_control, field, alignment_status, coalesce(value_text, ''), "
    "coalesce(value_number::VARCHAR, ''), coalesce(period_end::VARCHAR, ''), coalesce(value_note, '') "
    "FROM (SELECT * FROM int_spine_county_context UNION ALL SELECT * FROM int_spine_linkage) "
    "WHERE alignment_status IN ('aligned', 'held_in_staging') ORDER BY ALL;"
)
AL4B_COUNT_SQL = (
    "SELECT 'context rows', (SELECT count(*) FROM int_spine_county_context)::VARCHAR UNION ALL "
    "SELECT 'context keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_county_context)::VARCHAR UNION ALL "
    "SELECT 'linkage rows', (SELECT count(*) FROM int_spine_linkage)::VARCHAR UNION ALL "
    "SELECT 'linkage keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_linkage)::VARCHAR UNION ALL "
    "SELECT measure_source || ' ' || alignment_status, count(*)::VARCHAR "
    "FROM (SELECT * FROM int_spine_county_context UNION ALL SELECT * FROM int_spine_linkage) GROUP BY 1 ORDER BY 1;"
)
COUNTY_COUNT_SQL = (
    "SELECT 'rows', (SELECT count(*) FROM int_spine_county_measures)::VARCHAR UNION ALL "
    "SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_county_measures)::VARCHAR UNION ALL "
    "SELECT measure_source || ' ' || alignment_status, count(*)::VARCHAR FROM int_spine_county_measures GROUP BY 1 ORDER BY 1;"
)
OPERATIONS_SQL = (
    "SELECT ccn, window_year::VARCHAR, measure_source, measure_control, alignment_status, coalesce(value_text, ''), "
    "coalesce(value_number::VARCHAR, ''), coalesce(period_end::VARCHAR, ''), coalesce(age_months::VARCHAR, ''), coalesce(unit_count::VARCHAR, ''), "
    "coalesce(skipped_count::VARCHAR, '') FROM int_spine_operations_measures WHERE alignment_status IN ('aligned', 'held_in_staging') ORDER BY ALL;"
)
OPERATIONS_COUNT_SQL = (
    "SELECT 'rows', (SELECT count(*) FROM int_spine_operations_measures)::VARCHAR UNION ALL "
    "SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_operations_measures)::VARCHAR UNION ALL "
    "SELECT alignment_status, count(*)::VARCHAR FROM int_spine_operations_measures GROUP BY 1 ORDER BY 1;"
)
HOSPITAL_SQL = (
    "SELECT ccn, window_year::VARCHAR, measure_source, measure_control, field, alignment_status, coalesce(value_code, ''), "
    "coalesce(value_number::VARCHAR, ''), coalesce(period_end::VARCHAR, ''), coalesce(rule_stage, ''), coalesce(age_months::VARCHAR, ''), "
    "coalesce(period_copy_count::VARCHAR, '') FROM int_spine_hospital_measures WHERE alignment_status IN ('aligned', 'held_in_staging') ORDER BY ALL;"
)
HOSPITAL_COUNT_SQL = (
    "SELECT 'rows', (SELECT count(*) FROM int_spine_hospital_measures)::VARCHAR UNION ALL "
    "SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_hospital_measures)::VARCHAR UNION ALL "
    "SELECT 'spine', (SELECT count(*) FROM int_hospital_spine)::VARCHAR UNION ALL "
    "SELECT alignment_status, count(*)::VARCHAR FROM int_spine_hospital_measures GROUP BY 1 ORDER BY 1;"
)
CARE_SQL = (
    "SELECT ccn, window_year::VARCHAR, measure_control, alignment_status, coalesce(value_text, ''), coalesce(value_number::VARCHAR, ''), "
    "coalesce(value_category, ''), coalesce(period_end::VARCHAR, ''), coalesce(age_months::VARCHAR, ''), coalesce(release_file_count::VARCHAR, '') "
    "FROM int_spine_care_compare_measures WHERE alignment_status <> 'not_in_source' ORDER BY ALL;"
)
CARE_COUNT_SQL = (
    "SELECT (SELECT count(*) FROM int_spine_care_compare_measures)::VARCHAR, "
    "(SELECT count(*) FROM int_hospital_spine)::VARCHAR || ' x ' || (SELECT count(*) FROM registry_measure_sources)::VARCHAR, "
    "(SELECT count(DISTINCT alignment_key) FROM int_spine_care_compare_measures)::VARCHAR;"
)
OUTCOME_COUNT_SQL = (
    "SELECT (SELECT count(*) FROM int_spine_hai_outcomes)::VARCHAR, (SELECT 6 * count(*) FROM int_hospital_spine)::VARCHAR, "
    "(SELECT count(DISTINCT outcome_key) FROM int_spine_hai_outcomes)::VARCHAR;"
)
PROGRAM_YEARS_SQL = (
    "SELECT 'hac', ccn, fiscal_year::VARCHAR, coalesce(total_hac_score, ''), coalesce(is_payment_reduced::VARCHAR, ''), "
    "coalesce(psi_90_value, ''), left(member_sha256, 3) FROM int_hac_program_years "
    "UNION ALL SELECT 'vbp', ccn, fiscal_year::VARCHAR, coalesce(total_performance_score, ''), '', '', left(member_sha256, 3) "
    "FROM int_vbp_program_years ORDER BY ALL;"
)
VBP_DOMAINS_SQL = (
    "SELECT ccn, fiscal_year::VARCHAR, coalesce(unweighted_normalized_clinical_care_domain_score, ''), "
    "coalesce(unweighted_normalized_clinical_outcomes_domain_score, ''), coalesce(weighted_person_and_community_engagement_domain_score, ''), "
    "coalesce(weighted_efficiency_and_cost_reduction_domain_score, '') FROM int_vbp_program_years ORDER BY ALL;"
)
PROGRAM_VALUES_SQL = (
    "SELECT measure_control, ccn, fiscal_year::VARCHAR, field, coalesce(value_text, ''), coalesce(value_number::VARCHAR, '') "
    "FROM int_validation_program_values ORDER BY ALL;"
)
PROGRAM_HOLDS_SQL = (
    "SELECT bronze_table, coalesce(ccn, ''), coalesce(fiscal_year::VARCHAR, ''), hold_reason, row_count::VARCHAR "
    "FROM int_validation_program_holds ORDER BY ALL;"
)
MMD_SQL = (
    "SELECT measure_control, coalesce(file_control, ''), data_year::VARCHAR, geography_level, coalesce(county_fips, ''), coalesce(state_fips, ''), "
    "coalesce(value_number::VARCHAR, ''), value_unit, denominator_band, is_possible_suppression::VARCHAR, is_unknown_county::VARCHAR, "
    "is_connecticut::VARCHAR FROM int_mmd_prevalence ORDER BY ALL;"
)
HPSA_SOURCES_SQL = "SELECT hpsa_id, county_source FROM int_hpsa_components WHERE county_source <> 'published' ORDER BY ALL;"
PLACES_SOURCES_SQL = "SELECT county_source, count(*)::VARCHAR FROM int_places_county_values GROUP BY 1 ORDER BY 1;"
HPSA_SQL = (
    "SELECT hpsa_id, capture_date::VARCHAR, coalesce(designation_date::VARCHAR, ''), coalesce(geography_id, ''), "
    "coalesce(county_fips, ''), coalesce(county_token, ''), "
    "coalesce(hpsa_score::VARCHAR, ''), hpsa_status, coalesce(withdrawn_date::VARCHAR, ''), is_connecticut::VARCHAR, coalesce(hold_reason, '') "
    "FROM int_hpsa_components ORDER BY ALL;"
)
MUA_SQL = (
    "SELECT mua_id, capture_date::VARCHAR, designation_type_code, coalesce(designation_date::VARCHAR, ''), component_type, coalesce(component_name, ''), "
    "coalesce(county_fips, ''), coalesce(county_token, ''), coalesce(imu_score::VARCHAR, ''), mua_status, coalesce(withdrawal_date::VARCHAR, ''), "
    "is_connecticut::VARCHAR, coalesce(hold_reason, '') FROM int_mua_components ORDER BY ALL;"
)


def per_table(query: str, prefix: str) -> str:
    """Return the query once per table, each run after setting the checked_table variable to the prefixed table name."""
    return "".join(f"SET VARIABLE checked_table = '{prefix}{table}';\n{query}" for table in TABLES)


def unprefixed(rows: list[list[str]], prefix: str) -> dict[str, list[str]]:
    """Key query rows by table name without its prefix."""
    return {row[0].removeprefix(prefix): row[1:] for row in rows}


class CasePool:
    """Parallel fixture builds: one memory and CPU reading split into equal shares, and admission per launch [687] [688].

    Every pooled container is capped at its share (Compose mem_limit), and a launch waits while the memory Docker and
    the Mac can give, plus what this pool's running containers use, is below one share for each running container and
    the new one; containers other projects start during the run are therefore counted [687].
    """

    def __init__(self, workers: int, plan: memory_budget.LaunchPlan) -> None:
        self.workers = workers
        self.share = plan.budget.free // workers
        self.threads = max(1, plan.threads // workers)
        self.prefix = f"hai-staging-e2e-{os.getpid()}"
        self.lock = threading.Lock()
        self.running = 0
        self.launches = 0
        self.environment = {
            **plan.environment(),
            "JOB_MEMORY_LIMIT": str(self.share),
            "DUCKDB_MEMORY_LIMIT": f"{(self.share * 4 // 5) // 1000**3}GB",
            "JOB_THREADS": str(self.threads),
        }

    def own_usage(self) -> int:
        """Return the memory this pool's running containers use now."""
        lines = memory_budget.docker("stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}").splitlines()
        return sum(memory_budget.parse_size(line.split(" ", 1)[1].split("/")[0]) for line in lines if line.startswith(self.prefix))

    def admit(self) -> tuple[str, dict[str, Any]]:
        """Wait until one more share fits, then reserve it and return the container name and the launch record."""
        while True:
            with self.lock:
                try:
                    free = memory_budget.current().free
                except memory_budget.BudgetError:
                    if self.running == 0:
                        raise
                    free = 0
                if free + self.own_usage() - self.running * self.share >= self.share or self.running == 0 and free >= self.share:
                    self.running += 1
                    self.launches += 1
                    record = {"launch": self.launches, "share": self.share, "threads": self.threads, "free_at_launch": free}
                    return f"{self.prefix}-{self.launches}", record
            time.sleep(5)

    def release(self) -> None:
        with self.lock:
            self.running -= 1

    def stop_containers(self) -> list[str]:
        """Stop this pool's containers only, never another project's or another run's [692]."""
        names = [name for name in memory_budget.docker("ps", "--format", "{{.Names}}").split() if name.startswith(self.prefix)]
        for name in names:
            run_command("docker", ["stop", name], timeout=120)
        return names


POOL: CasePool | None = None
# Seconds each fixture case took to build, for the report [694].
CASE_SECONDS: dict[str, float] = {}


def compose_run(service_args: list[str], extra_env: dict[str, str]) -> tuple[int, str, str]:
    """Run one container of the analytics-dbt service and return its exit code and output; inside the case pool, with its share."""
    case = extra_env.get("STAGING_E2E_CASE", "query_or_setup")
    pool = POOL
    if pool is None:
        plan = memory_budget.launch_plan()
        environment, name, record = plan.environment(), None, plan.record()
    else:
        name, record = pool.admit()
        environment = pool.environment
    extra_env = {**extra_env, **environment}
    BUDGETS.append({"case": case, **record})
    env_flags = [flag for item, value in extra_env.items() for flag in ("-e", f"{item}={value}")]
    args = ["compose", "--project-directory", str(REPO_ROOT), "-f", str(REPO_ROOT / "docker-compose.yaml"), "--env-file", str(catalog.COMPOSE_ENV)]
    args += ["--profile", "query", "run", "--rm", "--no-deps", "-T", "--quiet-pull", *(["--name", name] if name else []), *env_flags, *service_args]
    try:
        result = run_command("docker", args, cwd=REPO_ROOT, env={**catalog.system_environment(), **environment}, timeout=TIMEOUT)
    finally:
        if pool is not None:
            pool.release()
    return result.returncode, result.stdout, result.stderr


DBT_SNAPSHOT = CASES / "_dbt_snapshot"
# Folders dbt writes or installs into; they are not the code under test [693].
DBT_GENERATED = ("target", "dbt_packages", "logs")


def dbt_tree_sha256() -> str:
    """Return one hash of every file in dbt/ except the generated folders, by path and content [693]."""
    digest = hashlib.sha256()
    root = REPO_ROOT / "dbt"
    for path in sorted(item for item in root.rglob("*") if item.is_file() and item.relative_to(root).parts[0] not in DBT_GENERATED):
        digest.update(str(path.relative_to(root)).encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def snapshot_dbt() -> None:
    """Copy dbt/ once; every fixture case copies this snapshot, so an edit during the run never reaches a case [693]."""
    if DBT_SNAPSHOT.exists():
        shutil.rmtree(DBT_SNAPSHOT)
    shutil.copytree(REPO_ROOT / "dbt", DBT_SNAPSHOT)


def build_cases(jobs: list[tuple[str, tuple[Stored, ...], frozenset[str]]], workers: int) -> dict[str, tuple[int, dict[str, str]] | BaseException]:
    """Build every fixture case, in a pool when there is room for two or more workers, and return each result or error.

    The checks run afterwards, in their own order, so the outcome does not depend on which build ends first [690]. One
    failed case is recorded against itself and the others continue [691]; an interrupt stops this pool's containers [692].
    """
    global POOL
    names = [case for case, _objects, _unlabelled in jobs]
    if len(set(names)) != len(names):
        raise ValueError(f"fixture case names repeat: {sorted(name for name in names if names.count(name) > 1)}")

    def one(job: tuple[str, tuple[Stored, ...], frozenset[str]]) -> tuple[int, dict[str, str]]:
        case, objects, unlabelled = job
        started = time.monotonic()
        try:
            return run_fixture(case, objects, unlabelled)
        finally:
            CASE_SECONDS[case] = round(time.monotonic() - started, 1)

    def outcome(future: Future[tuple[int, dict[str, str]]]) -> tuple[int, dict[str, str]] | BaseException:
        # A case's error is recorded against that case and the other cases continue [691].
        error = future.exception()
        return error if error is not None else future.result()

    plan = memory_budget.launch_plan()
    count = min(workers, plan.threads, plan.budget.free // memory_budget.FLOOR)
    if count < 2:
        with ThreadPoolExecutor(max_workers=1) as serial:
            futures = {job[0]: serial.submit(one, job) for job in jobs}
            return {case: outcome(future) for case, future in futures.items()}
    POOL = CasePool(count, plan)
    executor = ThreadPoolExecutor(max_workers=count)
    try:
        futures = {job[0]: executor.submit(one, job) for job in jobs}
        return {case: outcome(future) for case, future in futures.items()}
    except BaseException:
        executor.shutdown(wait=False, cancel_futures=True)
        POOL.stop_containers()
        raise
    finally:
        executor.shutdown(wait=True)
        POOL = None


def built(results: dict[str, tuple[int, dict[str, str]] | BaseException], case: str) -> tuple[int, dict[str, str]]:
    """Return a case's build result; a case that raised stops the run here, with its error [691]."""
    result = results[case]
    if isinstance(result, BaseException):
        raise RuntimeError(f"fixture {case} failed: {result}") from result
    return result


def duckdb_csv(database: str, sql: str, init: str | None = None) -> list[list[str]]:
    """Query a database in the case folder with the DuckDB CLI and return the rows without the header."""
    init_flags = ["-cmd", ".read /opt/analytics/resources.sql", *(["-init", init] if init else [])]
    code, stdout, stderr = compose_run(["--entrypoint", "duckdb", "analytics-dbt", database, *init_flags, "-csv", "-noheader", "-c", sql], {})
    if code:
        raise RuntimeError(f"duckdb query failed: {stderr.strip().splitlines()[-1:] or ['no output']}")
    return [row for row in csv.reader(io.StringIO(stdout)) if row]


def install_packages() -> None:
    """Install the dbt packages from dbt/package-lock.yml into the output mount; the project mount stays read-only."""
    code, stdout, stderr = compose_run(["analytics-dbt", "deps"], {})
    if code:
        raise SystemExit(f"dbt deps failed ({code}): {(stdout + stderr)[-2000:]}")


def dbt_build(case: str, target: str, project: bool = False) -> tuple[int, dict[str, str]]:
    """Run dbt build for a case, on its own project copy when asked, and return the exit code and each node's status."""
    case_dir = f"{CONTAINER_OUT}/e2e/{case}"
    env = {"STAGING_E2E_CASE": case_dir, "DBT_TARGET_PATH": f"{case_dir}/target", "DBT_LOG_PATH": f"{case_dir}/logs"}
    flags = ["--project-dir", f"{case_dir}/project", "--profiles-dir", f"{case_dir}/project"] if project else []
    # A build that dies leaves no results of its own; never read the previous run's.
    (CASES / case / "target/run_results.json").unlink(missing_ok=True)
    code, _, _ = compose_run(["analytics-dbt", "build", "--target", target, *flags], env)
    results_path = CASES / case / "target/run_results.json"
    if not results_path.exists():
        return code, {}
    results = json.loads(results_path.read_text())
    return code, {item["unique_id"].split(".")[2]: item["status"] for item in results["results"]}


def generator_objects(objects: Iterable[Stored]) -> list[ipps_file_labels.Stored]:
    """Return the fixture's IPPS and occupational-mix objects as the label generator sees them."""
    return [
        ipps_file_labels.Stored(item.table, item.key, item.sha, item.snapshot, FIXTURE_CONTAINER, tuple(item.member.split("!")))
        for item in objects
        if item.table in ipps_file_labels.TABLES
    ]


def fixture_project(case_dir: Path, objects: tuple[Stored, ...], unlabelled: frozenset[str]) -> None:
    """Copy the dbt project into the case and write its label and twin seeds with the real generator functions."""
    project = case_dir / "project"
    shutil.copytree(DBT_SNAPSHOT if DBT_SNAPSHOT.exists() else REPO_ROOT / "dbt", project)
    stored = generator_objects(objects)
    rows = [row for row in ipps_file_labels.labels(stored, {}) if row["object_key"] not in unlabelled]
    (project / "seeds/ipps_occmix_copy_labels.csv").write_text(ipps_file_labels.as_csv(rows, ipps_file_labels.LABEL_COLUMNS))
    twin_rows = ipps_file_labels.twins(stored, FIXTURE_RENAMED)
    (project / "seeds/ipps_occmix_twins.csv").write_text(ipps_file_labels.as_csv(twin_rows, ipps_file_labels.TWIN_COLUMNS))
    (project / "seeds/pos_file_periods.csv").write_text(periods_csv(objects, unlabelled))
    (project / "seeds/ownership_release_periods.csv").write_text(ownership_periods_csv(objects, unlabelled))
    (project / "seeds/geography_file_periods.csv").write_text(geography_periods_csv(objects, unlabelled))
    (project / "seeds/acs_variable_map.csv").write_text(acs_map_csv(FIXTURE_ACS_MAP))


def run_fixture(case: str, objects: tuple[Stored, ...], unlabelled: frozenset[str] = frozenset()) -> tuple[int, dict[str, str]]:
    """Write a case's fixture bronze database and project copy and build the models against them."""
    case_dir = CASES / case
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    fixture_project(case_dir, objects, unlabelled)
    (case_dir / "bronze.csv").write_text(fixture_csv(objects))
    (case_dir / "copies.csv").write_text(copies_csv(objects))
    variables = f"SET VARIABLE fixture_csv = '{CONTAINER_OUT}/e2e/{case}/bronze.csv';\nSET VARIABLE copies_csv = '{CONTAINER_OUT}/e2e/{case}/copies.csv';\n"
    for table in WIDE_COLUMNS:
        (case_dir / f"{table}.csv").write_text(wide_csv(objects, table))
        variables += f"SET VARIABLE {table}_csv = '{CONTAINER_OUT}/e2e/{case}/{table}.csv';\n"
    (case_dir / "column_map.csv").write_text(column_map_csv(objects))
    (case_dir / "file_preambles.csv").write_text(file_preambles_csv(objects))
    variables += f"SET VARIABLE file_preambles_csv = '{CONTAINER_OUT}/e2e/{case}/file_preambles.csv';\n"
    variables += f"SET VARIABLE column_map_csv = '{CONTAINER_OUT}/e2e/{case}/column_map.csv';\n"
    (case_dir / "bronze.sql").write_text(variables + FIXTURE_SQL)
    database = f"{CONTAINER_OUT}/e2e/{case}/fixture_lakehouse.duckdb"
    code, _, stderr = compose_run(
        [
            "--entrypoint",
            "duckdb",
            "analytics-dbt",
            database,
            "-cmd",
            ".read /opt/analytics/resources.sql",
            "-c",
            f".read {CONTAINER_OUT}/e2e/{case}/bronze.sql",
        ],
        {},
    )
    if code:
        raise RuntimeError(f"fixture {case} failed: {stderr.strip().splitlines()[-1:] or ['no output']}")
    return dbt_build(case, "fixture", project=True)


def model_outputs(case: str) -> dict[str, Any]:
    """Read a fixture case's copy and file models and each staging view's row count; a missing model is an error entry."""
    try:
        return read_models(case)
    except RuntimeError as error:
        return {"error": str(error)}


def read_models(case: str) -> dict[str, Any]:
    """Read a fixture case's copy and file models and each staging view's row count."""
    database = f"{CONTAINER_OUT}/e2e/{case}/staging.duckdb"
    attach = CASES / case / "attach.sql"
    attach.write_text(f"ATTACH '{CONTAINER_OUT}/e2e/{case}/fixture_lakehouse.duckdb' AS lakehouse (READ_ONLY);\n")
    init = f"{CONTAINER_OUT}/e2e/{case}/attach.sql"
    rows = {table: int(values[0]) for table, values in unprefixed(duckdb_csv(database, per_table(VIEW_ROWS_SQL, "stg_"), init), "stg_").items()}
    return {
        "copies": [tuple(row) for row in duckdb_csv(database, COPIES_SQL)],
        "files": [tuple(row) for row in duckdb_csv(database, FILES_SQL)],
        "rows": rows,
        "twins": [tuple(row) for row in duckdb_csv(database, TWINS_SQL)],
        "selection": [tuple(row) for row in duckdb_csv(database, SELECTION_SQL)],
        "sheets": [tuple(row) for row in duckdb_csv(database, SHEETS_SQL)],
        "windows": [tuple(row) for row in duckdb_csv(database, WINDOWS_SQL)],
        "holds": [tuple(row) for row in duckdb_csv(database, HOLDS_SQL)],
        "pos": [tuple(row) for row in duckdb_csv(database, POS_SQL)],
        "cmi_rows": [tuple(row) for row in duckdb_csv(database, CMI_ROWS_SQL)],
        "cmi_years": [tuple(row) for row in duckdb_csv(database, CMI_YEARS_SQL)],
        "cmi_holds": [tuple(row) for row in duckdb_csv(database, CMI_HOLDS_SQL)],
        "cmi_data_years": [tuple(row) for row in duckdb_csv(database, CMI_DATA_YEARS_SQL)],
        "spine": [tuple(row) for row in duckdb_csv(database, SPINE_SQL)],
        "pos_history": [tuple(row) for row in duckdb_csv(database, POS_HISTORY_SQL)],
        "owner_history": [tuple(row) for row in duckdb_csv(database, OWNER_HISTORY_SQL)],
        "hgi_history": [tuple(row) for row in duckdb_csv(database, HGI_HISTORY_SQL)],
        "timely": [tuple(row) for row in duckdb_csv(database, TE_SQL)],
        "maternal": [tuple(row) for row in duckdb_csv(database, MATERNAL_SQL)],
        "hcahps": [tuple(row) for row in duckdb_csv(database, HCAHPS_SQL)],
        "cc_holds": [tuple(row) for row in duckdb_csv(database, CC_HOLDS_SQL)],
        "registry": [tuple(row) for row in duckdb_csv(database, REGISTRY_SQL)],
        "hgi": [tuple(row) for row in duckdb_csv(database, HGI_SQL)],
        "cost_reports": [tuple(row) for row in duckdb_csv(database, COST_REPORTS_SQL)],
        "cost_measures": [tuple(row) for row in duckdb_csv(database, COST_MEASURES_SQL)],
        "impact_values": [tuple(row) for row in duckdb_csv(database, IMPACT_VALUES_SQL)],
        "impact_measures": [tuple(row) for row in duckdb_csv(database, IMPACT_MEASURES_SQL)],
        "impact_holds": [tuple(row) for row in duckdb_csv(database, IMPACT_HOLDS_SQL)],
        "mup_providers": [tuple(row) for row in duckdb_csv(database, MUP_PROVIDERS_SQL)],
        "mup_drg": [tuple(row) for row in duckdb_csv(database, MUP_DRG_SQL)],
        "mup_measures": [tuple(row) for row in duckdb_csv(database, MUP_MEASURES_SQL)],
        "owners": [tuple(row) for row in duckdb_csv(database, OWNERS_SQL)],
        "enrollments": [tuple(row) for row in duckdb_csv(database, ENROLLMENTS_SQL)],
        "enrollment_ccn_sources": [tuple(row) for row in duckdb_csv(database, ENROLLMENT_SOURCES_SQL)],
        "chow": [tuple(row) for row in duckdb_csv(database, CHOW_SQL)],
        "hhs": [tuple(row) for row in duckdb_csv(database, HHS_SQL)],
        "hhs_sources": [tuple(row) for row in duckdb_csv(database, HHS_SOURCES_SQL)],
        "onc_chpl": [tuple(row) for row in duckdb_csv(database, ONC_CHPL_SQL)],
        "onc_attestations": [tuple(row) for row in duckdb_csv(database, ONC_ATTESTATIONS_SQL)],
        "phone_columns": [tuple(row) for row in duckdb_csv(database, PHONE_COLUMNS_SQL)],
        "occmix": [tuple(row) for row in duckdb_csv(database, OCCMIX_SQL)],
        "hud": [tuple(row) for row in duckdb_csv(database, HUD_SQL)],
        "hud_holds": [tuple(row) for row in duckdb_csv(database, HUD_HOLDS_SQL)],
        "adjacency": [tuple(row) for row in duckdb_csv(database, ADJACENCY_SQL)],
        "rucc": [tuple(row) for row in duckdb_csv(database, RUCC_SQL)],
        "ruca": [tuple(row) for row in duckdb_csv(database, RUCA_SQL)],
        "hsa": [tuple(row) for row in duckdb_csv(database, HSA_SQL)],
        "acs": [tuple(row) for row in duckdb_csv(database, ACS_SQL)],
        "svi": [tuple(row) for row in duckdb_csv(database, SVI_SQL)],
        "saipe": [tuple(row) for row in duckdb_csv(database, SAIPE_SQL)],
        "sahie": [tuple(row) for row in duckdb_csv(database, SAHIE_SQL)],
        "bls": [tuple(row) for row in duckdb_csv(database, BLS_SQL)],
        "places": [tuple(row) for row in duckdb_csv(database, PLACES_SQL)],
        "places_detail": [tuple(row) for row in duckdb_csv(database, PLACES_DETAIL_SQL)],
        "gv": [tuple(row) for row in duckdb_csv(database, GV_SQL)],
        "wonder": [tuple(row) for row in duckdb_csv(database, WONDER_SQL)],
        "mmd": [tuple(row) for row in duckdb_csv(database, MMD_SQL)],
        "d1_windows": [tuple(row) for row in duckdb_csv(database, D1_WINDOWS_SQL)],
        "validation": [tuple(row) for row in duckdb_csv(database, VALIDATION_SQL)],
        "validation_aliases": [tuple(row) for row in duckdb_csv(database, VALIDATION_ALIASES_SQL)],
        "validation_holds": [tuple(row) for row in duckdb_csv(database, VALIDATION_HOLDS_SQL)],
        "program_years": [tuple(row) for row in duckdb_csv(database, PROGRAM_YEARS_SQL)],
        "program_values": [tuple(row) for row in duckdb_csv(database, PROGRAM_VALUES_SQL)],
        "vbp_domains": [tuple(row) for row in duckdb_csv(database, VBP_DOMAINS_SQL)],
        "program_holds": [tuple(row) for row in duckdb_csv(database, PROGRAM_HOLDS_SQL)],
        "hai_outcomes": [tuple(row) for row in duckdb_csv(database, OUTCOME_SQL)],
        "care_compare": [tuple(row) for row in duckdb_csv(database, CARE_SQL)],
        "hospital_measures": [tuple(row) for row in duckdb_csv(database, HOSPITAL_SQL)],
        "operations_measures": [tuple(row) for row in duckdb_csv(database, OPERATIONS_SQL)],
        "validation_aligned": [tuple(row) for row in duckdb_csv(database, VALIDATION_ALIGNED_SQL)],
        "county_measures": [tuple(row) for row in duckdb_csv(database, COUNTY_SQL)],
        "county_measure_counts": [tuple(row) for row in duckdb_csv(database, COUNTY_COUNT_SQL)],
        "al4b_context": [tuple(row) for row in duckdb_csv(database, AL4B_SQL)],
        "al4b_context_counts": [tuple(row) for row in duckdb_csv(database, AL4B_COUNT_SQL)],
        "validation_aligned_counts": [tuple(row) for row in duckdb_csv(database, VALIDATION_ALIGNED_COUNT_SQL)],
        "operations_measure_counts": [tuple(row) for row in duckdb_csv(database, OPERATIONS_COUNT_SQL)],
        "hospital_measure_counts": [tuple(row) for row in duckdb_csv(database, HOSPITAL_COUNT_SQL)],
        "care_compare_counts": [tuple(row) for row in duckdb_csv(database, CARE_COUNT_SQL)],
        "hai_outcome_counts": [tuple(row) for row in duckdb_csv(database, OUTCOME_COUNT_SQL)],
        "hai_outcome_status": [tuple(row) for row in duckdb_csv(database, OUTCOME_STATUS_SQL)],
        "hai_outcome_status_counts": [tuple(row) for row in duckdb_csv(database, OUTCOME_STATUS_COUNT_SQL)],
        "hai_outcome_values": [tuple(row) for row in duckdb_csv(database, OUTCOME_VALUES_SQL)],
        "hai_outcome_value_rows": [tuple(row) for row in duckdb_csv(database, OUTCOME_VALUE_ROWS_SQL)],
        "hpsa": [tuple(row) for row in duckdb_csv(database, HPSA_SQL)],
        "hpsa_sources": [tuple(row) for row in duckdb_csv(database, HPSA_SOURCES_SQL)],
        "places_sources": [tuple(row) for row in duckdb_csv(database, PLACES_SOURCES_SQL)],
        "mua": [tuple(row) for row in duckdb_csv(database, MUA_SQL)],
        "occmix_holds": [
            tuple(row)
            for row in duckdb_csv(
                database, "SELECT hold_reason, count(*)::VARCHAR FROM int_occmix_survey_rows WHERE hold_reason IS NOT NULL GROUP BY ALL ORDER BY ALL;"
            )
        ],
        "occmix_measures": [
            tuple(row)
            for row in duckdb_csv(
                database, "SELECT measure_control, count(*)::VARCHAR, count(value_number)::VARCHAR FROM int_occmix_measures GROUP BY ALL ORDER BY ALL;"
            )
        ],
    }


def refuses(action: Any, fragment: str) -> bool:
    """Return whether the action raises the generator's error with the fragment in its message."""
    try:
        action()
    except ipps_file_labels.LabelError as error:
        return fragment in str(error)
    return False


def refuses_geography(action: Any, fragment: str) -> bool:
    """Return whether the action raises the geography period generator's error with the fragment in its message."""
    try:
        action()
    except geography_file_periods.PeriodError as error:
        return fragment in str(error)
    return False


def refuses_period(action: Any, fragment: str) -> bool:
    """Return whether the action raises the ownership period generator's error with the fragment in its message."""
    try:
        action()
    except ownership_release_periods.PeriodError as error:
        return fragment in str(error)
    return False


def geography_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's C1 geography models with their expected rows and check the period generator's refusals."""
    checks: dict[str, bool] = {}
    # [400] to [416] C1 geography: non-county HUD rows held, Connecticut and territories flagged, ratios typed; adjacency
    # self-links, islands and 2010 continuation lines; RUCC and RUCA codes as text with blank and 99 codes null; service-area
    # suppression kept as flags.
    checks["hud_matches_expected"] = base.get("hud") == sorted(
        [
            ("2021Q1", "00501", "36103", "state", "false", "0.0", "1.0", "false"),
            ("2021Q1", "01001", "25013", "state", "false", "1.0", "1.0", "true"),
            ("2021Q1", "06001", "09110", "state", "true", "1.0", "1.0", "true"),
            ("2021Q1", "53001", "55117", "state", "false", "1.0", "0.9995", "true"),
            ("2021Q1", "20001", "11001", "state", "false", "0.75", "0.75", "true"),
            ("2021Q1", "20001", "24031", "state", "false", "0.25", "0.25", "true"),
            ("2021Q1", "00601", "72001", "territory", "false", "1.0", "1.0", "true"),
            ("2020Q4", "01001", "25013", "state", "false", "1.0", "1.0", "true"),
            ("2020Q4", "35004", "01073", "state", "false", "1.0", "1.0", "true"),
        ]
    )
    checks["hud_holds_match_expected"] = base.get("hud_holds") == sorted(
        [("2021Q1", "96799", "60", "not_county_code"), ("2021Q1", "53001", "99999", "not_county_code")]
    )
    checks["adjacency_matches_expected"] = base.get("adjacency") == sorted(
        [
            ("2025", "01001", "Autauga County, AL", "01021", "12345.6", "false", "false", "false"),
            ("2025", "01021", "Chilton County, AL", "01001", "12345.6", "false", "false", "false"),
            ("2025", "15007", "Kauai County, HI", "", "", "false", "true", "false"),
            ("2025", "09190", "Western Connecticut Planning Region, CT", "09110", "0.0", "false", "false", "true"),
            ("2025", "09110", "Capitol Planning Region, CT", "09190", "0.0", "false", "false", "true"),
            ("2024", "01001", "Autauga County, AL", "01001", "", "true", "false", "false"),
            ("2024", "01001", "Autauga County, AL", "01021", "", "false", "false", "false"),
            ("2024", "01021", "Chilton County, AL", "01001", "", "false", "false", "false"),
            ("2024", "01021", "Chilton County, AL", "01021", "", "true", "false", "false"),
            ("2010", "01001", "Autauga County, AL", "01001", "", "true", "false", "false"),
            ("2010", "01001", "Autauga County, AL", "01021", "", "false", "false", "false"),
            ("2010", "01021", "Chilton County, AL", "01001", "", "false", "false", "false"),
            ("2010", "01021", "Chilton County, AL", "01021", "", "true", "false", "false"),
            ("2010", "27165", "", "27013", "", "false", "false", "false"),
            ("2010", "27165", "", "27165", "", "true", "false", "false"),
            ("2010", "27013", "Blue Earth County, MN", "27165", "", "false", "false", "false"),
        ]
    )
    checks["rucc_matches_expected"] = base.get("rucc") == sorted(
        [
            ("2023", "01001", "2", "2", "58805.0", "false"),
            ("2023", "09120", "1", "1", "902412.0", "true"),
            ("2023", "09001", "", "", "957419.0", "true"),
            ("2013", "01001", "2", "2.0", "54571.0", "false"),
            ("2013", "02105", "", "", "2150.0", "false"),
        ]
    )
    checks["ruca_matches_expected"] = base.get("ruca") == sorted(
        [
            ("2020", "tract", "01001020100", "01001", "1", "1", "1", "1"),
            ("2020", "tract", "09001010101", "09001", "2", "2.1", "2", "2.1"),
            ("2020", "tract", "01001990000", "01001", "", "", "99", "99"),
            ("2020", "zip", "00501", "", "1", "1", "1", "1"),
            ("2020", "zip", "99950", "", "10", "10.3", "10", "10.3"),
            ("2010", "zip", "00501", "", "1", "1.1", "1", "1.1"),
            ("2010", "tract", "01001020100", "01001", "1", "1", "1", "1"),
            ("2010", "tract", "72153750602", "72153", "4", "4.1", "4", "4.1"),
        ]
    )
    checks["hsa_matches_expected"] = base.get("hsa") == sorted(
        [
            ("2015", "010001", "true", "32420", "false", "false", "23.0", "false", "915149.0", "false"),
            ("2015", "010001", "true", "", "true", "false", "", "false", "", "false"),
            ("2015", "010001", "true", "", "true", "false", "", "false", "", "false"),
            ("2015", "010001", "true", "", "false", "true", "4.0", "false", "90.0", "false"),
            ("2016", "010001", "true", "32420", "false", "false", "", "true", "", "true"),
            ("2016", "01T001", "true", "32421", "false", "false", "12.0", "false", "50000.0", "false"),
            ("2016", "10001", "false", "32422", "false", "false", "15.0", "false", "1000.0", "false"),
        ]
    )
    checks["geography_seed_matches_registry"] = geography_seed_matches()
    checks["geography_generator_refuses_file_without_period"] = refuses_geography(
        lambda: geography_file_periods.rows_for(
            [{"table": "hud_zip_county", "sha256": sha("q9"), "release_id": "missing", "file_name": "crosswalk.csv"}], {}, {}
        ),
        "no recorded quarter",
    )
    checks["geography_generator_refuses_two_periods"] = refuses_geography(
        lambda: geography_file_periods.receipt_quarter(
            {
                "snapshot_id": "two",
                "release": {"publisher_release_label": "2021Q1"},
                "measurement_periods": [
                    {"source_basis": "publisher_stated", "start_date": "2021-01-01", "end_date": "2021-03-31"},
                    {"source_basis": "publisher_stated", "start_date": "2021-04-01", "end_date": "2021-06-30"},
                ],
            }
        ),
        "more than one",
    )
    checks["geography_generator_refuses_unreviewed_vintage"] = refuses_geography(
        lambda: geography_file_periods.rows_for([{"table": "rucc", "sha256": sha("q8"), "release_id": "r", "file_name": "2033-rucc.csv"}], {}, {}),
        "no reviewed vintage",
    )
    return checks


def county_context_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's C2 ACS and SVI models with their expected rows and check the vintage generator's refusals."""
    checks: dict[str, bool] = {}
    # [420] to [431] The mapped column per vintage, county rows only, tokens kept, the top-coded median capped and flagged;
    # held concepts give nothing; SVI -999 null with its token, the 2000 label row left out.
    checks["acs_matches_expected"] = base.get("acs") == sorted(
        [
            ("2014", "DP04_0078PE", "dp04_0077pe", "01001", "2.9", "", "false"),
            ("2023", "DP04_0078PE", "dp04_0078pe", "01001", "3.1", "", "false"),
            ("2023", "DP04_0078PE", "dp04_0078pe", "09110", "1.2", "", "false"),
            ("2023", "DP04_0078PE", "dp04_0078pe", "72001", "", "(X)", "false"),
            ("2023", "S1701_C03_001E", "s1701_c03_001e", "01001", "15.2", "", "false"),
            ("2023", "S1701_C03_001E", "s1701_c03_001e", "35039", "", "null", "false"),
            ("2017", "B19013_001E", "b19013_001e", "01001", "250000.0", "", "true"),
            ("2017", "B19013_001E", "b19013_001e", "48301", "", "-", "false"),
            ("2017", "B19013_001M", "b19013_001m", "01001", "", "***", "false"),
            ("2017", "B19013_001M", "b19013_001m", "48301", "", "**", "false"),
            ("2018", "B19013_001E", "b19013_001e", "01001", "58000.0", "", "false"),
            ("2018", "B19013_001M", "b19013_001m", "01001", "1200.0", "", "false"),
            ("2023", "B19013_001E", "b19013_e001", "01001", "62000.0", "", "false"),
            ("2023", "B19013_001E", "b19013_e001", "01003", "", "-999999999", "false"),
            ("2023", "B19013_001M", "b19013_m001", "01001", "1500.0", "", "false"),
            ("2023", "B19013_001M", "b19013_m001", "01003", "", "-222222222", "false"),
        ]
    )
    checks["svi_matches_expected"] = base.get("svi") == sorted(
        [
            ("2022", "01001", "e_totpop", "58761.0", ""),
            ("2022", "01001", "ep_pov150", "20.2", ""),
            ("2022", "01001", "ep_unemp", "", "-999"),
            ("2022", "01001", "rpl_themes", "0.4", ""),
            ("2022", "09120", "e_totpop", "100.0", ""),
            ("2022", "09120", "ep_pov150", "10.0", ""),
            ("2022", "09120", "rpl_themes", "0.2", ""),
            ("2000", "01001", "g1v1r", "15.2", ""),
        ]
    )
    checks["acs_svi_seed_matches_registry"] = acs_svi_seed_matches()
    checks["vintage_generator_refuses_acs_name_without_year"] = refuses_geography(
        lambda: geography_file_periods.rows_for([{"table": "acs_dp04", "sha256": sha("q7"), "release_id": "r", "file_name": "DP04-Data.csv"}], {}, {}),
        "no publisher vintage",
    )
    checks["vintage_generator_refuses_svi_without_edition"] = refuses_geography(
        lambda: geography_file_periods.rows_for([{"table": "svi", "sha256": sha("q6"), "release_id": "r", "file_name": "SVI.csv"}], {}, {}, {}),
        "no recorded edition",
    )
    return checks


def income_labor_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's C3 SAIPE, SAHIE and BLS models with their expected rows and check the vintage generator's refusals."""
    checks: dict[str, bool] = {}
    # [435] to [445] County rows only; the Alabama-only twin not typed; `.` and `-` null with the field or token kept; the
    # annual average flagged; footnote codes kept; the capture date as the BLS vintage.
    checks["saipe_matches_expected"] = base.get("saipe") == sorted(
        [
            ("2023", "01001", "7004.0", "11.7", "68857.0", "62667.0", ""),
            ("1999", "01001", "4991.0", "11.4", "39702.0", "37226.0", ""),
            ("1999", "01003", "12000.0", "10.1", "", "", "median_household_income|median_household_income_lb90|median_household_income_ub90"),
        ]
    )
    checks["sahie_matches_expected"] = base.get("sahie") == sorted(
        [
            ("2022", "01001", "0", "0", "0", "0", "true", "45000.0", "4000.0", "8.9", "1.2", ""),
            ("2022", "01001", "1", "0", "0", "0", "false", "30000.0", "3500.0", "11.7", "1.5", ""),
            ("2022", "15005", "0", "0", "0", "0", "true", "", "", "", "", "nipr|nui|pctui|pctui_moe"),
        ]
    )
    checks["bls_matches_expected"] = base.get("bls") == sorted(
        [
            ("01001", "unemployment_rate", "2023", "1", "false", "false", "3.1", "", "", "2026-09-26"),
            ("01001", "unemployment_rate", "2023", "", "true", "false", "2.8", "", "", "2026-09-26"),
            ("01001", "unemployed", "2025", "10", "false", "false", "", "-", "X", "2026-09-26"),
            ("01001", "labor_force", "2023", "1", "false", "false", "26000.0", "", "", "2026-09-26"),
        ]
    )
    checks["income_labor_seed_matches_registry"] = income_labor_seed_matches()
    checks["vintage_generator_refuses_saipe_name_without_year"] = refuses_geography(
        lambda: geography_file_periods.rows_for([{"table": "saipe_text_lines", "sha256": sha("q5"), "release_id": "r", "file_name": "estall.txt"}], {}, {}),
        "no publisher year",
    )
    checks["vintage_generator_refuses_bls_without_capture_date"] = refuses_geography(
        lambda: geography_file_periods.rows_for([{"table": "bls_laus", "sha256": sha("q4"), "release_id": "BLS", "file_name": "observations.csv"}], {}, {}),
        "no capture date",
    )
    return checks


def county_health_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's C4 PLACES, geographic variation and WONDER models with their expected rows and check the generator's refusal."""
    checks: dict[str, bool] = {}
    # [449] to [459] County rows only; the all-state flag only where all 51 are covered; geographic variation long with *
    # kept; WONDER databases apart, the shorter export's identical 2024 row held, marks kept as tokens.
    checks["places_matches_expected"] = base.get("places") == [
        ("2020", "2018", "DIABETES", "CrdPrv", "1", "1", "false"),
        ("2025", "2022", "LONELINESS", "CrdPrv", "1", "1", "false"),
        ("2025", "2022", "OBESITY", "CrdPrv", "51", "51", "true"),
        ("2025", "2023", "ARTHRITIS", "CrdPrv", "2", "2", "false"),
        ("2025", "2023", "DIABETES", "AgeAdjPrv", "1", "1", "false"),
        ("2025", "2023", "DIABETES", "CrdPrv", "2", "1", "false"),
    ]
    # [623] [624] One 2020-layout row typed through the name crosswalk; the unknown and two-code names stay untyped.
    checks["places_sources_match_expected"] = base.get("places_sources") == [("places_name_crosswalk", "1"), ("published", "57")]
    checks["places_detail_matches_expected"] = base.get("places_detail") == [
        ("01001", "DIABETES", "AgeAdjPrv", "10.5", "", "", "false"),
        ("01001", "DIABETES", "CrdPrv", "11.0", "", "", "false"),
        ("01001", "DIABETES", "CrdPrv", "12.1", "11.0", "", "false"),
        ("01003", "DIABETES", "CrdPrv", "", "", "*", "false"),
        ("01005", "ARTHRITIS", "CrdPrv", "20.0", "", "", "false"),
        ("01007", "ARTHRITIS", "CrdPrv", "21.0", "", "", "false"),
        ("09120", "LONELINESS", "CrdPrv", "30.2", "", "", "true"),
    ]
    checks["gv_matches_expected"] = base.get("gv") == sorted(
        [
            ("2020", "01073", "ma_prtcptn_rate", "0.3", ""),
            ("2023", "01001", "bene_dual_pct", "", "*"),
            ("2023", "01001", "benes_total_cnt", "9000.0", ""),
            ("2023", "01001", "ma_prtcptn_rate", "0.45", ""),
            ("2023", "01001", "pqi03_dbts_age_65_74", "", "NA"),
            ("2022", "01001", "ma_prtcptn_rate", "0.44", ""),
        ]
    )
    checks["wonder_matches_expected"] = base.get("wonder") == sorted(
        [
            (BRIDGED, "01001", "2018", "500.0", "909.1", "", ""),
            (BRIDGED, "01003", "2018", "", "", "Suppressed", ""),
            (SINGLE_RACE, "01001", "2018", "500.0", "907.4", "", ""),
            (SINGLE_RACE, "01001", "2024", "520.0", "", "Unreliable", ""),
            (SINGLE_RACE, "01001", "2024", "520.0", "", "Unreliable", "repeated_in_wider_export"),
        ]
    )
    checks["county_health_seed_matches_registry"] = county_health_seed_matches()
    checks["vintage_generator_refuses_wonder_without_database"] = refuses_geography(
        lambda: geography_file_periods.rows_for(
            [{"table": "wonder_county_mortality", "sha256": sha("q3"), "release_id": "r", "file_name": "county_year.csv"}], {}, {}
        ),
        "no recorded database",
    )
    return checks


def shortage_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's C5 MMD, HPSA and MUA models with their expected rows and check the condition map's refusals."""
    checks: dict[str, bool] = {}
    # [468] to [473] Controls from the label map, C258.01 from its earlier capture; codes padded; state rows apart; zeros
    # flagged; the unknown county and Connecticut flagged; the per-100,000 unit only for C258.78.
    checks["mmd_matches_expected"] = base.get("mmd") == sorted(
        [
            ("C258.02", "C258.02", "2020", "county", "01073", "01", "11.0", "percent", "10,000+", "false", "false", "false"),
            ("C258.01", "", "2022", "county", "01001", "01", "0.8", "percent", "1,000-4,999", "false", "false", "false"),
            ("C258.02", "C258.02", "2023", "county", "01001", "01", "12.5", "percent", "1,000-4,999", "false", "false", "false"),
            ("C258.02", "C258.02", "2023", "county", "01003", "01", "0.0", "percent", "11-499", "true", "false", "false"),
            ("C258.02", "C258.02", "2023", "county", "09001", "09", "10.1", "percent", "10,000+", "false", "false", "true"),
            ("C258.02", "C258.02", "2023", "county", "09990", "09", "3.0", "percent", "11-499", "false", "true", "true"),
            ("C258.78", "C258.78", "2023", "state", "", "01", "170.0", "per_100000", "10,000+", "false", "false", "false"),
        ]
    )
    # [475] to [479] The exact repeat held; both designations of a reused ID kept; XXXXX kept as a token; dates and scores typed.
    # [626] [627] The routes that filled a county.
    checks["hpsa_sources_match_expected"] = base.get("hpsa_sources") == [("103", "geography_id"), ("107", "hud_zip_single_county")]
    checks["hpsa_matches_expected"] = base.get("hpsa") == sorted(
        [
            ("101", "2026-09-24", "2013-08-13", "01001", "01001", "", "15", "Designated", "", "false", ""),
            ("101", "2026-09-24", "2013-08-13", "01001", "01001", "", "15", "Designated", "", "false", "exact_repeat"),
            ("102", "2026-09-24", "2008-10-08", "01003", "01003", "", "7", "Withdrawn", "2013-06-27", "false", ""),
            ("102", "2026-09-24", "2013-08-13", "01003", "01003", "", "14", "Withdrawn", "2018-07-02", "false", ""),
            # [626] A Connecticut tract ID gives its county; [626] a tract ID from another state and [627] a facility postal code.
            ("103", "2026-09-24", "2022-01-05", "09110010100", "09110", "XXXXX", "20", "Designated", "", "true", ""),
            ("106", "2026-09-24", "2022-01-05", "25025000100", "", "XXXXX", "12", "Designated", "", "true", ""),
            ("107", "2026-09-24", "2022-01-05", "POINT (-86.8 33.5)", "25013", "XXX", "18", "Designated", "", "false", ""),
            ("104", "2026-09-24", "2000-02-01", "01005", "01005", "", "3", "Withdrawn", "", "false", ""),
            ("108", "2026-09-24", "2015-05-01", "01073", "01073", "", "9", "Withdrawn", "", "false", ""),
        ]
    )
    checks["mua_matches_expected"] = base.get("mua") == sorted(
        [
            ("00001", "2026-09-24", "MUA", "1978-11-01", "Single County", "Autauga", "01001", "", "44.1", "Withdrawn", "2001-01-26", "false", ""),
            ("00001", "2026-09-24", "MUA", "1994-01-01", "Single County", "Autauga", "01001", "", "52.9", "Designated", "", "false", ""),
            ("00001", "2026-09-24", "MUA", "1994-01-01", "Single County", "Autauga", "01001", "", "52.9", "Designated", "", "false", "exact_repeat"),
            ("00518", "2026-09-24", "MUA", "2001-11-22", "Single County", "Pasco", "12101", "", "54.9", "Designated", "", "false", ""),
            ("00518", "2026-09-24", "MUP", "2001-11-22", "Single County", "Pasco", "12101", "", "50.3", "Withdrawn", "2009-02-26", "false", ""),
            ("00700", "2026-09-24", "MUA", "2005-10-28", "Census Tract", "113.02", "", "XXXXX", "61.3", "Designated", "", "true", ""),
        ]
    )
    checks["shortage_seed_matches_registry"] = shortage_seed_matches()
    checks["condition_map_refuses_shared_label"] = refuses_condition(
        lambda: mmd_conditions.rows_for(
            [
                {"measure_id": "C258.01", "condition_code": "2", "condition_label": AMI, "geography": "c"},
                {"measure_id": "C258.02", "condition_code": "1", "condition_label": AMI, "geography": "c"},
            ],
            {"C258.01": "", "C258.02": ""},
        ),
        "names both",
    )
    checks["vintage_generator_refuses_mmd_name_without_year"] = refuses_geography(
        lambda: geography_file_periods.rows_for([{"table": "cms_mmd_csv", "sha256": sha("q6"), "release_id": "r", "file_name": "mmd_extract.csv"}], {}, {}),
        "no publisher year",
    )
    checks["vintage_generator_refuses_hpsa_without_capture_date"] = refuses_geography(
        lambda: geography_file_periods.rows_for(
            [{"table": "hrsa_hpsa_detail", "sha256": sha("q7"), "release_id": "HPSA", "file_name": "BCD_HPSA_FCT_DET_PC.csv"}], {}, {}
        ),
        "no capture date",
    )
    checks["condition_map_refuses_unregistered_control"] = refuses_condition(
        lambda: mmd_conditions.rows_for([{"measure_id": "C258.99", "condition_code": "9", "condition_label": "x", "geography": "c"}], {}),
        "not in the registry",
    )
    return checks


def validation_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's D1 window and validation models with their expected rows [504] to [511]."""
    checks: dict[str, bool] = {}
    # [505] [506] [507] [524] Older-layout windows read; the later release supersedes the older OP_32; HRRP keyed by
    # measure_name; the repeated MORT_30_HF window is held, not kept; the 5-digit ID is padded and the malformed one held.
    checks["d1_windows_match_expected"] = base.get("d1_windows") == sorted(
        [
            ("visits", "010001", "OP_32", "2020-07-01", "11.5", "r9r"),
            ("visits", "010001", "OP_32", "2021-01-01", "11.0", "r9r"),
            ("visits", "010001", "OP_32", "2022-01-01", "12.5", "vi1"),
            ("visits", "010001", "OP-32", "2022-01-01", "13.0", "vi1"),
            ("visits", "010003", "OP-32", "2022-01-01", "4.0", "vi1"),
            ("visits", "010001", "READM_30_HF", "2022-01-01", "20.1", "vi1"),
            ("visits", "010003", "OP_35_ED", "2022-01-01", "Not Available", "vi1"),
            ("visits", "010003", "OP_36", "2022-01-01", "7.0", "vi1"),
            ("visits", "010001", "OP_36", "2020-01-01", "9.9", "vi0"),
            ("deaths", "010001", "MORT_30_AMI", "2022-01-01", "12.3", "de1"),
            ("deaths", "010001", "PSI_90", "2022-01-01", "1.01", "de1"),
            ("deaths", "010001", "PSI_90_SAFETY", "2022-01-01", "0.99", "de1"),
            ("hrrp", "010001", "READM-30-HF-HRRP", "2020-07-01", "1.0123", "hr1"),
            ("hrrp", "010003", "READM-30-AMI-HRRP", "2020-07-01", "N/A", "hr1"),
        ]
    )
    # [508] [509] [510] [629] Exact IDs, plus OP-32 where its hospital and window have no OP_32 row (010003); OP-32 beside OP_32
    # (010001), READM_30_HF and the PSI IDs stay out; parents and children both; tokens stay text.
    checks["validation_matches_expected"] = base.get("validation") == sorted(
        [
            ("C290", "010001", "OP_32", "2020-07-01", "11.5", "11.5"),
            ("C290", "010001", "OP_32", "2021-01-01", "11.0", "11.0"),
            ("C290.01", "010001", "OP_32", "2020-07-01", "11.5", "11.5"),
            ("C290.01", "010001", "OP_32", "2021-01-01", "11.0", "11.0"),
            ("C289", "010001", "READM-30-HF-HRRP", "2020-07-01", "1.0123", "1.0123"),
            ("C289", "010003", "READM-30-AMI-HRRP", "2020-07-01", "N/A", ""),
            ("C289.01", "010003", "READM-30-AMI-HRRP", "2020-07-01", "N/A", ""),
            ("C289.04", "010001", "READM-30-HF-HRRP", "2020-07-01", "1.0123", "1.0123"),
            ("C290", "010001", "OP_32", "2022-01-01", "12.5", "12.5"),
            ("C290", "010001", "OP_36", "2020-01-01", "9.9", "9.9"),
            ("C290", "010003", "OP_35_ED", "2022-01-01", "Not Available", ""),
            ("C290", "010003", "OP_36", "2022-01-01", "7.0", "7.0"),
            ("C290.01", "010001", "OP_32", "2022-01-01", "12.5", "12.5"),
            ("C290.03", "010003", "OP_35_ED", "2022-01-01", "Not Available", ""),
            ("C290.04", "010001", "OP_36", "2020-01-01", "9.9", "9.9"),
            ("C290.04", "010003", "OP_36", "2022-01-01", "7.0", "7.0"),
            ("C291", "010001", "MORT_30_AMI", "2022-01-01", "12.3", "12.3"),
            ("C291.01", "010001", "MORT_30_AMI", "2022-01-01", "12.3", "12.3"),
            ("C290", "010003", "OP_32", "2022-01-01", "4.0", "4.0"),
            ("C290.01", "010003", "OP_32", "2022-01-01", "4.0", "4.0"),
        ]
    )
    checks["validation_aliases_match_expected"] = base.get("validation_aliases") == [("010003", "OP_32", "OP-32")]
    checks["validation_holds_match_expected"] = base.get("validation_holds") == [
        ("cms_cc_complications_and_deaths_hospital", "010001", "MORT_30_HF", "repeated_in_file", "2"),
        ("cms_cc_unplanned_hospital_visits_hospital", "", "", "no_key", "1"),
    ]
    checks["validation_seed_matches_registry"] = validation_seed_matches()
    return checks


def county_measure_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's AL4a rows with their expected values [604] to [611]."""
    checks: dict[str, bool] = {}
    # [604] The latest data year before the start through the POS county: 2020 GV and MMD values for 010001's county; WONDER
    # 2018 from the single-race export (907.4, not the bridged 909.1); RUCC 2013. Reviewed row by row.
    checks["county_measures_match_expected"] = base.get("county_measures") == [
        ("010001", "2021", "geographic_variation", "C252", "ma_prtcptn_rate", "aligned", "0.3", "0.3", "2020-12-31", "1", "true"),
        ("010001", "2021", "mmd", "C258.02", "prevalence", "aligned", "11.0", "11.0", "2020-12-31", "1", "true"),
        ("01001F", "2021", "rucc", "C198", "rucc_code", "aligned", "2", "", "2013-12-31", "85", "true"),
        ("01001F", "2021", "wonder", "C259", "crude_rate", "aligned", "907.4", "907.4", "2018-12-31", "25", "true"),
        ("01001F", "2021", "wonder", "C259", "deaths", "aligned", "500.0", "500.0", "2018-12-31", "25", "true"),
        ("01001F", "2021", "wonder", "C259", "population", "aligned", "55100.0", "55100.0", "2018-12-31", "25", "true"),
    ]
    # [611] One row per spine row and control-field (12 x 195), unique keys, and the counts per source and status.
    checks["county_measure_counts_match_expected"] = base.get("county_measure_counts") == [
        ("geographic_variation aligned", "1"),
        ("geographic_variation no_period_before_start", "3"),
        ("geographic_variation not_in_source", "296"),
        ("keys", "2925"),
        ("mmd aligned", "1"),
        ("mmd no_period_before_start", "6"),
        ("mmd not_in_source", "1193"),
        ("places no_period_before_start", "2"),
        ("places not_in_source", "1288"),
        ("rows", "2925"),
        ("rucc aligned", "1"),
        ("rucc no_period_before_start", "1"),
        ("rucc not_in_source", "13"),
        ("wonder aligned", "3"),
        ("wonder no_period_before_start", "1"),
        ("wonder not_in_source", "116"),
    ]
    return checks


# AL4b, reviewed row by row: [634] ACS vintage 2018 and 2014 and SAIPE 1999 before the start; [636] HPSA and MUA in force
# for 01001F (score as published at the capture), 0 elsewhere with none in force, and 010001's county held by an undated
# withdrawal; [638] the service area from 2015, the latest year with unsuppressed rows (2016's cases are suppressed).
AL4B_EXPECTED: list[tuple[str, ...]] = [
    ("010001", "2019", "hpsa", "C249", "designations_in_force", "held_in_staging", "", "", "2018-12-31", ""),
    ("010001", "2019", "hpsa", "C250", "highest_score", "held_in_staging", "", "", "2018-12-31", ""),
    ("010001", "2019", "hsa", "L001", "service_area_cases", "aligned", "23.0", "23.0", "2015-12-31", "published_counts_only"),
    ("010001", "2019", "hsa", "L001", "service_area_zips", "aligned", "1.0", "1.0", "2015-12-31", "published_counts_only"),
    ("010001", "2019", "mua", "C251", "designations_in_force", "aligned", "0", "0.0", "2018-12-31", ""),
    ("010001", "2019", "mua", "C251", "highest_score", "aligned", "", "", "2018-12-31", "none_in_force"),
    ("010001", "2021", "hpsa", "C249", "designations_in_force", "held_in_staging", "", "", "2020-12-31", ""),
    ("010001", "2021", "hpsa", "C250", "highest_score", "held_in_staging", "", "", "2020-12-31", ""),
    ("010001", "2021", "hsa", "L001", "service_area_cases", "aligned", "23.0", "23.0", "2015-12-31", "published_counts_only"),
    ("010001", "2021", "hsa", "L001", "service_area_zips", "aligned", "1.0", "1.0", "2015-12-31", "published_counts_only"),
    ("010001", "2021", "mua", "C251", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("010001", "2021", "mua", "C251", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
    ("010001", "2025", "hsa", "L001", "service_area_cases", "aligned", "23.0", "23.0", "2015-12-31", "published_counts_only"),
    ("010001", "2025", "hsa", "L001", "service_area_zips", "aligned", "1.0", "1.0", "2015-12-31", "published_counts_only"),
    ("010005", "2019", "hpsa", "C249", "designations_in_force", "aligned", "0", "0.0", "2018-12-31", ""),
    ("010005", "2019", "hpsa", "C250", "highest_score", "aligned", "", "", "2018-12-31", "none_in_force"),
    ("010005", "2019", "mua", "C251", "designations_in_force", "aligned", "0", "0.0", "2018-12-31", ""),
    ("010005", "2019", "mua", "C251", "highest_score", "aligned", "", "", "2018-12-31", "none_in_force"),
    ("010005", "2021", "hpsa", "C249", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("010005", "2021", "hpsa", "C250", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
    ("010005", "2021", "mua", "C251", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("010005", "2021", "mua", "C251", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
    ("01001F", "2021", "acs", "C176", "B19013_001E", "aligned", "58000", "58000.0", "2018-12-31", ""),
    ("01001F", "2021", "acs", "C176", "B19013_001M", "aligned", "1200", "1200.0", "2018-12-31", ""),
    ("01001F", "2021", "acs", "C181", "DP04_0078PE", "aligned", "2.9", "2.9", "2014-12-31", ""),
    ("01001F", "2021", "hpsa", "C249", "designations_in_force", "aligned", "1", "1.0", "2020-12-31", ""),
    ("01001F", "2021", "hpsa", "C250", "highest_score", "aligned", "15.0", "15.0", "2020-12-31", "score_as_published_at_capture"),
    ("01001F", "2021", "mua", "C251", "designations_in_force", "aligned", "1", "1.0", "2020-12-31", ""),
    ("01001F", "2021", "mua", "C251", "highest_score", "aligned", "52.9", "52.9", "2020-12-31", "score_as_published_at_capture"),
    ("01001F", "2021", "saipe", "C200", "poverty_all_count", "aligned", "4991.0", "4991.0", "1999-12-31", ""),
    ("01001F", "2021", "saipe", "C200", "poverty_all_pct", "aligned", "11.4", "11.4", "1999-12-31", ""),
    ("01001F", "2021", "saipe", "C200", "poverty_all_pct_lb90", "aligned", "8.9", "8.9", "1999-12-31", ""),
    ("01001F", "2021", "saipe", "C200", "poverty_all_pct_ub90", "aligned", "14.0", "14.0", "1999-12-31", ""),
    ("01001F", "2021", "saipe", "C201", "median_household_income", "aligned", "39702.0", "39702.0", "1999-12-31", ""),
    ("01001F", "2021", "saipe", "C201", "median_household_income_lb90", "aligned", "37226.0", "37226.0", "1999-12-31", ""),
    ("01001F", "2021", "saipe", "C201", "median_household_income_ub90", "aligned", "42342.0", "42342.0", "1999-12-31", ""),
    ("011301", "2020", "hpsa", "C249", "designations_in_force", "aligned", "0", "0.0", "2019-12-31", ""),
    ("011301", "2020", "hpsa", "C250", "highest_score", "aligned", "", "", "2019-12-31", "none_in_force"),
    ("011301", "2020", "mua", "C251", "designations_in_force", "aligned", "0", "0.0", "2019-12-31", ""),
    ("011301", "2020", "mua", "C251", "highest_score", "aligned", "", "", "2019-12-31", "none_in_force"),
    ("011301", "2020", "saipe", "C200", "poverty_all_count", "aligned", "12000.0", "12000.0", "1999-12-31", ""),
    ("011301", "2020", "saipe", "C200", "poverty_all_pct", "aligned", "10.1", "10.1", "1999-12-31", ""),
    ("011301", "2020", "saipe", "C200", "poverty_all_pct_lb90", "aligned", "8.5", "8.5", "1999-12-31", ""),
    ("011301", "2020", "saipe", "C200", "poverty_all_pct_ub90", "aligned", "11.7", "11.7", "1999-12-31", ""),
    ("011301", "2021", "hpsa", "C249", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("011301", "2021", "hpsa", "C250", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
    ("011301", "2021", "mua", "C251", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("011301", "2021", "mua", "C251", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
    ("011301", "2021", "saipe", "C200", "poverty_all_count", "aligned", "12000.0", "12000.0", "1999-12-31", ""),
    ("011301", "2021", "saipe", "C200", "poverty_all_pct", "aligned", "10.1", "10.1", "1999-12-31", ""),
    ("011301", "2021", "saipe", "C200", "poverty_all_pct_lb90", "aligned", "8.5", "8.5", "1999-12-31", ""),
    ("011301", "2021", "saipe", "C200", "poverty_all_pct_ub90", "aligned", "11.7", "11.7", "1999-12-31", ""),
    ("070001", "2021", "hpsa", "C249", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("070001", "2021", "hpsa", "C250", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
    ("070001", "2021", "mua", "C251", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("070001", "2021", "mua", "C251", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
    ("210001", "2021", "hpsa", "C249", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("210001", "2021", "hpsa", "C250", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
    ("210001", "2021", "mua", "C251", "designations_in_force", "aligned", "0", "0.0", "2020-12-31", ""),
    ("210001", "2021", "mua", "C251", "highest_score", "aligned", "", "", "2020-12-31", "none_in_force"),
]
# [611] Context 14 spine rows x 155 control-fields, linkage 14 x 3, unique keys, and the counts per source and status.
AL4B_COUNTS_EXPECTED: list[tuple[str, ...]] = [
    ("acs aligned", "3"),
    ("acs no_period_before_start", "7"),
    ("acs not_in_source", "1145"),
    ("bls no_period_before_start", "2"),
    ("bls not_in_source", "118"),
    ("context keys", "2325"),
    ("context rows", "2325"),
    ("hpsa aligned", "14"),
    ("hpsa held_in_staging", "4"),
    ("hpsa not_in_source", "12"),
    ("hsa aligned", "6"),
    ("hsa not_in_source", "24"),
    ("hud no_period_before_start", "1"),
    ("hud not_in_source", "14"),
    ("linkage keys", "45"),
    ("linkage rows", "45"),
    ("mua aligned", "18"),
    ("mua not_in_source", "12"),
    ("ruca not_in_source", "90"),
    ("sahie no_period_before_start", "4"),
    ("sahie not_in_source", "56"),
    ("saipe aligned", "15"),
    ("saipe not_in_source", "90"),
    ("svi no_period_before_start", "6"),
    ("svi not_in_source", "729"),
]


def al4b_context_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's AL4b rows with their expected values [604] to [611] and [634] to [638]."""
    checks: dict[str, bool] = {}
    checks["al4b_context_match_expected"] = base.get("al4b_context") == AL4B_EXPECTED
    checks["al4b_context_counts_match_expected"] = base.get("al4b_context_counts") == AL4B_COUNTS_EXPECTED
    return checks


def validation_aligned_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's AL5 rows with their expected values [597] to [601]."""
    checks: dict[str, bool] = {}
    # [597] The OP_32 window equal to 2021 beats the longer one that covers it; [598] HAC FY 2023 placed by its HAI period
    # (calendar 2021); HRRP's three-year window overlaps 2021 fully; hospitals whose periods miss a window are
    # no_matching_period. Reviewed row by row.
    checks["validation_aligned_match_expected"] = base.get("validation_aligned") == [
        ("010001", "2019", "C285", "payment_reduction", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C286", "total_hac_score", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C289", "READM-30-HF-HRRP", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C289.04", "READM-30-HF-HRRP", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C290", "OP_32", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C290", "OP_36", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C290.01", "OP_32", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C290.04", "OP_36", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C291", "MORT_30_AMI", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2019", "C291.01", "MORT_30_AMI", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2021", "C285", "payment_reduction", "aligned", "No", "", "2021-01-01", "2021-12-31", "2023", "365"),
        ("010001", "2021", "C286", "total_hac_score", "aligned", "4.0", "4.0", "2021-01-01", "2021-12-31", "2023", "365"),
        ("010001", "2021", "C289", "READM-30-HF-HRRP", "aligned", "1.0123", "1.0123", "2020-07-01", "2023-06-30", "", "365"),
        ("010001", "2021", "C289.04", "READM-30-HF-HRRP", "aligned", "1.0123", "1.0123", "2020-07-01", "2023-06-30", "", "365"),
        ("010001", "2021", "C290", "OP_32", "aligned", "11.0", "11.0", "2021-01-01", "2021-12-31", "", "365"),
        ("010001", "2021", "C290", "OP_36", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2021", "C290.01", "OP_32", "aligned", "11.0", "11.0", "2021-01-01", "2021-12-31", "", "365"),
        ("010001", "2021", "C290.04", "OP_36", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2021", "C291", "MORT_30_AMI", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2021", "C291.01", "MORT_30_AMI", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C285", "payment_reduction", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C286", "total_hac_score", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C289", "READM-30-HF-HRRP", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C289.04", "READM-30-HF-HRRP", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C290", "OP_32", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C290", "OP_36", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C290.01", "OP_32", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C290.04", "OP_36", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C291", "MORT_30_AMI", "no_matching_period", "", "", "", "", "", ""),
        ("010001", "2025", "C291.01", "MORT_30_AMI", "no_matching_period", "", "", "", "", "", ""),
    ]
    # [600] One row per spine row and seed row (12 x 41), unique keys; [599] no model reads the validation table.
    checks["validation_aligned_counts_match_expected"] = base.get("validation_aligned_counts") == [
        ("aligned", "6"),
        ("keys", "615"),
        ("no_matching_period", "24"),
        ("not_in_source", "585"),
        ("rows", "615"),
    ]
    checks["validation_table_kept_apart"] = validation_kept_apart()
    return checks


def operations_measure_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's AL3b rows with their expected values [587] to [595]."""
    checks: dict[str, bool] = {}
    # [587] [588] HHS 2020 weeks only: C260 = (50 + 60) / (100 + 100), E035.01 = 3 + 4; [592] [593] the 2020 owner release
    # through the enrollment CCN: private equity Y from a direct owner, the managing organisation's REIT flag not counted;
    # [591] ONC's latest period; [594] no change of ownership in 2024 and 24 months since the latest. Reviewed row by row.
    held = [
        ("010005", "2021", "hhs_capacity", control, "held_in_staging", "", "", "2020-12-31", "1", "0", "1")
        for control in [
            "C260",
            "C261",
            "C262.01",
            "C262.02",
            "C262.03",
            "C262.04",
            "C262.05",
            "C262.06",
            "C263.01",
            "C263.02",
            "C263.03",
            "C263.04",
            "C263.05",
            "C263.06",
            "E034.01",
            "E034.02",
            "E034.03",
            "E034.04",
            "E034.05",
            "E034.06",
            "E034.07",
            "E034.08",
            "E034.09",
            "E034.10",
            "E034.11",
            "E034.12",
            "E034.13",
            "E034.14",
            "E034.15",
            "E034.16",
            "E034.17",
            "E034.18",
            "E034.19",
            "E034.20",
            "E034.21",
            "E034.22",
            "E034.23",
            "E034.24",
            "E034.25",
            "E034.26",
            "E035.01",
        ]
    ]
    checks["operations_measures_match_expected"] = base.get("operations_measures") == sorted(
        [
            ("010001", "2021", "hhs_capacity", "C260", "aligned", "", "0.55", "2020-12-31", "1", "2", "0"),
            ("010001", "2021", "hhs_capacity", "E035.01", "aligned", "", "7.0", "2020-12-31", "1", "2", "0"),
            ("010001", "2021", "ownership", "C067", "aligned", "Y", "1.0", "2020-07-31", "6", "2", "0"),
            ("010001", "2021", "ownership", "C068", "aligned", "not_reported", "0.0", "2020-07-31", "6", "2", "0"),
            ("010001", "2021", "ownership", "L005", "aligned", "O20000000001", "1.0", "2020-07-31", "6", "2", "0"),
            ("010001", "2025", "onc", "C071", "aligned", "Y", "", "2023-12-31", "13", "1", "0"),
            ("010001", "2025", "onc", "C072", "aligned", "Fixture Developer A", "1.0", "2023-12-31", "13", "1", "0"),
            ("010001", "2025", "onc", "C073", "aligned", "", "1.0", "2023-12-31", "13", "1", "0"),
            ("010001", "2025", "onc", "C074", "aligned", "0015EFIXTURE01", "", "2023-12-31", "13", "1", "0"),
            ("010001", "2025", "ownership", "C067", "aligned", "Y", "1.0", "2020-07-31", "54", "2", "0"),
            ("010001", "2025", "ownership", "C068", "aligned", "not_reported", "0.0", "2020-07-31", "54", "2", "0"),
            ("010001", "2025", "ownership", "C069", "aligned", "", "0.0", "2024-12-31", "1", "1", "0"),
            ("010001", "2025", "ownership", "C070", "aligned", "", "24.0", "2023-01-01", "24", "1", "0"),
            ("010001", "2025", "ownership", "L005", "aligned", "O20000000001", "1.0", "2020-07-31", "54", "2", "0"),
            *held,
        ]
    )
    # [589] 010005's only 2020 week has two HHS hospitals, so every HHS control is held; [595] one row per control (12 x 52).
    checks["operations_measure_counts_match_expected"] = base.get("operations_measure_counts") == [
        ("aligned", "14"),
        ("held_in_staging", "41"),
        ("keys", "780"),
        ("no_period_before_start", "156"),
        ("not_in_source", "569"),
        ("rows", "780"),
    ]
    return checks


def hospital_measure_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's AL3a rows with their expected values [576] to [586]."""
    checks: dict[str, bool] = {}
    # [576] to [581] Every aligned or held row: the latest cost report before the start; the IPPS rule year that ends before
    # it, most final stage first, with same-stage files that disagree held (C025, C038); the Medicare inpatient data year;
    # the occupational-mix survey. Reviewed row by row against the rules before being fixed here.
    checks["hospital_measures_match_expected"] = base.get("hospital_measures") == [
        ("010001", "2019", "ipps_impact", "C001", "beds", "aligned", "", "50.0", "2015-09-30", "correction+final", "40", "1"),
        ("010001", "2019", "ipps_impact", "C006", "resident_to_bed_ratio", "aligned", "", "0.28515", "2009-09-30", "unspecified", "112", "1"),
        ("010001", "2019", "ipps_impact", "C021", "geographic_labor_market_area", "aligned", "20020", "", "2009-09-30", "unspecified", "112", "1"),
        ("010001", "2019", "ipps_impact", "C021", "urgeo", "aligned", "OURBAN", "", "2009-09-30", "unspecified", "112", "1"),
        ("010001", "2019", "ipps_impact", "C022", "dsh_patient_percentage", "aligned", "", "0.1274", "2009-09-30", "unspecified", "112", "1"),
        ("010001", "2019", "ipps_impact", "C023", "medicare_percentage", "aligned", "", "0.273", "2009-09-30", "unspecified", "112", "1"),
        ("010001", "2019", "ipps_impact", "C025", "wage_index", "held_in_staging", "", "", "2015-09-30", "correction+final", "40", "1"),
        ("010001", "2019", "ipps_impact", "C026", "capital_ime_factor", "aligned", "", "0.028", "2009-09-30", "unspecified", "112", "1"),
        ("010001", "2019", "ipps_impact", "C026", "operating_ime_factor", "aligned", "", "0.05944", "2009-09-30", "unspecified", "112", "1"),
        ("010001", "2019", "ipps_impact", "C038", "medicare_bills", "held_in_staging", "", "", "2015-09-30", "correction+final", "40", "1"),
        ("010001", "2019", "ipps_impact", "C040", "average_daily_census", "aligned", "", "0.654054054054054", "2009-09-30", "unspecified", "112", "1"),
        ("010001", "2021", "ipps_impact", "C001", "beds", "aligned", "", "50.0", "2015-09-30", "correction+final", "64", "1"),
        ("010001", "2021", "ipps_impact", "C006", "resident_to_bed_ratio", "aligned", "", "0.28515", "2009-09-30", "unspecified", "136", "1"),
        ("010001", "2021", "ipps_impact", "C021", "geographic_labor_market_area", "aligned", "20020", "", "2009-09-30", "unspecified", "136", "1"),
        ("010001", "2021", "ipps_impact", "C021", "urgeo", "aligned", "OURBAN", "", "2009-09-30", "unspecified", "136", "1"),
        ("010001", "2021", "ipps_impact", "C022", "dsh_patient_percentage", "aligned", "", "0.1274", "2009-09-30", "unspecified", "136", "1"),
        ("010001", "2021", "ipps_impact", "C023", "medicare_percentage", "aligned", "", "0.273", "2009-09-30", "unspecified", "136", "1"),
        ("010001", "2021", "ipps_impact", "C025", "wage_index", "held_in_staging", "", "", "2015-09-30", "correction+final", "64", "1"),
        ("010001", "2021", "ipps_impact", "C026", "capital_ime_factor", "aligned", "", "0.028", "2009-09-30", "unspecified", "136", "1"),
        ("010001", "2021", "ipps_impact", "C026", "operating_ime_factor", "aligned", "", "0.05944", "2009-09-30", "unspecified", "136", "1"),
        ("010001", "2021", "ipps_impact", "C038", "medicare_bills", "held_in_staging", "", "", "2015-09-30", "correction+final", "64", "1"),
        ("010001", "2021", "ipps_impact", "C040", "average_daily_census", "aligned", "", "0.654054054054054", "2009-09-30", "unspecified", "136", "1"),
        ("010001", "2025", "cost_report", "C001", "value", "aligned", "", "250.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C002", "value", "aligned", "2", "", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C003", "value", "aligned", "1", "", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C023", "value", "aligned", "", "0.5", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C024", "value", "aligned", "", "0.1", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C027", "value", "aligned", "", "1500.5", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C028", "value", "aligned", "", "6.002", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C035", "value", "aligned", "", "4380000.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C037", "value", "aligned", "", "73000.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C038", "value", "aligned", "", "14600.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C039", "value", "aligned", "", "200.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C040", "value", "aligned", "", "0.8", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C041", "value", "aligned", "", "58.4", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C042", "value", "aligned", "", "5.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C045", "value", "aligned", "", "0.124", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C046", "value", "aligned", "", "2.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C047", "value", "aligned", "", "0.5", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C048", "value", "aligned", "", "30.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C049", "value", "aligned", "", "0.0125", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C050", "value", "aligned", "", "0.02", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C051", "value", "aligned", "", "-10.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C052", "value", "aligned", "", "0.1", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C053", "value", "aligned", "", "0.5", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C054", "value", "aligned", "", "0.01", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C055", "value", "aligned", "", "0.25", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "cost_report", "C057", "value", "aligned", "", "30000.0", "2023-09-30", "", "16", "1"),
        ("010001", "2025", "ipps_impact", "C001", "beds", "aligned", "", "50.0", "2015-09-30", "correction+final", "112", "1"),
        ("010001", "2025", "ipps_impact", "C006", "resident_to_bed_ratio", "aligned", "", "0.28515", "2009-09-30", "unspecified", "184", "1"),
        ("010001", "2025", "ipps_impact", "C021", "geographic_labor_market_area", "aligned", "20020", "", "2009-09-30", "unspecified", "184", "1"),
        ("010001", "2025", "ipps_impact", "C021", "urgeo", "aligned", "OURBAN", "", "2009-09-30", "unspecified", "184", "1"),
        ("010001", "2025", "ipps_impact", "C022", "dsh_patient_percentage", "aligned", "", "0.1274", "2009-09-30", "unspecified", "184", "1"),
        ("010001", "2025", "ipps_impact", "C023", "medicare_percentage", "aligned", "", "0.273", "2009-09-30", "unspecified", "184", "1"),
        ("010001", "2025", "ipps_impact", "C025", "wage_index", "held_in_staging", "", "", "2015-09-30", "correction+final", "112", "1"),
        ("010001", "2025", "ipps_impact", "C026", "capital_ime_factor", "aligned", "", "0.028", "2009-09-30", "unspecified", "184", "1"),
        ("010001", "2025", "ipps_impact", "C026", "operating_ime_factor", "aligned", "", "0.05944", "2009-09-30", "unspecified", "184", "1"),
        ("010001", "2025", "ipps_impact", "C038", "medicare_bills", "held_in_staging", "", "", "2015-09-30", "correction+final", "112", "1"),
        ("010001", "2025", "ipps_impact", "C040", "average_daily_census", "aligned", "", "0.654054054054054", "2009-09-30", "unspecified", "184", "1"),
        ("010001", "2025", "medicare_inpatient", "C038", "tot_dschrgs", "aligned", "", "1350.0", "2024-12-31", "", "1", "1"),
        ("010001", "2025", "medicare_inpatient", "C042", "tot_days", "aligned", "", "5.0", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C076", "bene_avg_risk_scre", "aligned", "", "1.8", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C077", "bene_avg_age", "aligned", "", "74.5", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C078", "bene_age_lt_65_cnt", "aligned", "", "0.1", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C079", "bene_age_65_74_cnt", "aligned", "", "0.4", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C080", "bene_age_75_84_cnt", "aligned", "", "0.3", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C081", "bene_age_gt_84_cnt", "aligned", "", "0.2", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C082", "bene_feml_cnt", "aligned", "", "0.55", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C083", "bene_dual_cnt", "aligned", "", "0.25", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C084", "bene_race_api_cnt", "aligned", "", "0.03", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C084", "bene_race_black_cnt", "aligned", "", "0.2", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C084", "bene_race_hspnc_cnt", "aligned", "", "0.05", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C084", "bene_race_othr_cnt", "aligned", "", "0.02", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C084", "bene_race_wht_cnt", "aligned", "", "0.7", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C085", "tot_benes", "aligned", "", "900.0", "2024-12-31", "", "1", "1"),
        ("010001", "2025", "medicare_inpatient", "C086", "tot_dschrgs", "aligned", "", "1.5", "2024-12-31", "", "1", "1"),
        ("010001", "2025", "medicare_inpatient", "C087", "tot_dschrgs", "aligned", "", "1350.0", "2024-12-31", "", "1", "1"),
        ("010001", "2025", "medicare_inpatient", "C088", "tot_cvrd_days", "aligned", "", "7400.0", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C089", "bene_cc_ph_diabetes_v2_pct", "aligned", "", "0.35", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C090", "bene_cc_ph_ckd_v2_pct", "aligned", "", "0.4", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C109", "bene_cc_bh_depress_v1_pct", "aligned", "", "0.3", "2023-12-31", "", "13", "1"),
        ("010001", "2025", "medicare_inpatient", "C117", "tot_dschrgs", "aligned", "", "0.06", "2023-12-31", "", "13", "1"),
        ("010005", "2019", "occupational_mix", "C030", "rnhr", "aligned", "", "100.0", "2016-12-31", "", "25", "1"),
        ("010005", "2019", "occupational_mix", "C031", "rn_paid_hour_share", "aligned", "", "1.0", "2016-12-31", "", "25", "1"),
        ("010005", "2019", "occupational_mix", "C032", "lpnst_paid_hour_share", "aligned", "", "0.0", "2016-12-31", "", "25", "1"),
        ("010005", "2019", "occupational_mix", "C033", "naorat_paid_hour_share", "aligned", "", "0.0", "2016-12-31", "", "25", "1"),
        ("010005", "2019", "occupational_mix", "C034", "rn_paid_hour_wage", "aligned", "", "30.0", "2016-12-31", "", "25", "1"),
        ("010005", "2021", "occupational_mix", "C030", "rnhr", "aligned", "", "100.0", "2016-12-31", "", "49", "1"),
        ("010005", "2021", "occupational_mix", "C031", "rn_paid_hour_share", "aligned", "", "1.0", "2016-12-31", "", "49", "1"),
        ("010005", "2021", "occupational_mix", "C032", "lpnst_paid_hour_share", "aligned", "", "0.0", "2016-12-31", "", "49", "1"),
        ("010005", "2021", "occupational_mix", "C033", "naorat_paid_hour_share", "aligned", "", "0.0", "2016-12-31", "", "49", "1"),
        ("010005", "2021", "occupational_mix", "C034", "rn_paid_hour_wage", "aligned", "", "30.0", "2016-12-31", "", "49", "1"),
    ]
    # [582] [583] One row per spine row and control-field (12 x 98), unique keys, and the counts per status.
    checks["hospital_measure_counts_match_expected"] = base.get("hospital_measure_counts") == [
        ("aligned", "86"),
        ("held_in_staging", "6"),
        ("keys", "1470"),
        ("no_period_before_start", "130"),
        ("not_in_source", "1248"),
        ("rows", "1470"),
        ("spine", "15"),
    ]
    return checks


def care_compare_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's AL2 rows with their expected values [564] to [575]."""
    checks: dict[str, bool] = {}
    blank = ("", "", "", "", "", "")

    def none_before(ccn: str, year: str, *controls: str) -> list[tuple[str, ...]]:
        return [(ccn, year, control, "no_period_before_start", *blank) for control in controls]

    # [564] [565] The latest period that ends before the start, its age in calendar months; [567] the status says why a
    # value is missing; [569] C141's spelling maps to one category; [571] [572] the rating from the release before the start,
    # held when two files on that date disagree; hospitals with no Care Compare row are not_in_source and not listed.
    checks["care_compare_match_expected"] = base.get("care_compare") == sorted(
        [
            ("010001", "2019", "C143", "aligned", "130", "130.0", "", "2018-12-31", "1", "1"),
            *none_before("010001", "2019", "C119", "C139", "C140", "C141", "C167", "C168", "C284"),
            ("010001", "2021", "C141", "aligned", "Low", "", "low", "2020-12-31", "1", "1"),
            ("010001", "2021", "C143", "aligned", "145", "145.0", "", "2020-12-31", "1", "1"),
            ("010001", "2021", "C284", "aligned", "2", "2.0", "", "2020-07-01", "6", "1"),
            *none_before("010001", "2021", "C119", "C139", "C140", "C167", "C168"),
            ("010001", "2025", "C119", "aligned", "80", "80.0", "", "2022-12-31", "25", "1"),
            ("010001", "2025", "C139", "aligned", "21", "21.0", "", "2022-12-31", "25", "1"),
            ("010001", "2025", "C140", "aligned", "507", "507.0", "", "2022-12-31", "25", "1"),
            ("010001", "2025", "C141", "aligned", "Low", "", "low", "2020-12-31", "49", "1"),
            ("010001", "2025", "C143", "aligned", "152", "152.0", "", "2022-12-31", "25", "1"),
            ("010001", "2025", "C149", "held_in_staging", "", "", "", "", "", ""),
            ("010001", "2025", "C167", "aligned", "Yes", "", "", "2023-12-31", "13", "1"),
            ("010001", "2025", "C168", "aligned", "30", "30.0", "", "2023-12-31", "13", "1"),
            ("010001", "2025", "C284", "held_in_staging", "", "", "", "2024-01-31", "12", "2"),
            *none_before("010002", "2019", "C141"),
            ("010005", "2021", "C143", "held_in_staging", "", "", "", "", "", ""),
            ("010005", "2021", "C284", "held_in_staging", "", "", "", "2020-10-01", "3", "2"),
            ("010005", "2019", "C284", "no_period_before_start", "", "", "", "", "", ""),
        ]
    )
    # [566] Every spine row has one row per control, and the key is unique.
    counts = base.get("care_compare_counts")
    row = counts[0] if isinstance(counts, list) and counts else ("", "", "")
    spine, controls = row[1].split(" x ") if " x " in row[1] else ("0", "0")
    checks["care_compare_one_row_per_control"] = row[0] == str(int(spine) * int(controls)) == row[2] and int(spine) > 0
    return checks


def outcome_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's AL1 outcome rows with their expected values [549] to [556]."""
    checks: dict[str, bool] = {}
    blank = ("", "", "", "", "", "")
    # [550] to [556] Published parts by exact ID; tokens stay text with no number; footnote codes from every format; the
    # late release held, not used; population flags from the spine; types with no row are not listed here.
    checks["hai_outcomes_match_expected"] = base.get("hai_outcomes") == sorted(
        [
            ("010001", "2019", "HAI_1", "0.6", "0.6", *blank, "0", "true"),
            ("010001", "2019", "HAI_2", "0.7", "0.7", *blank, "0", "true"),
            ("010001", "2021", "HAI_1", "0.4", "0.4", *blank, "0", "true"),
            ("010001", "2025", "HAI_1", "1.2", "1.2", *blank, "0", "false"),
            ("010002", "2019", "HAI_1", "0.9", "0.9", *blank, "0", "false"),
            ("010005", "2021", "HAI_1", "0.6", "0.6", *blank, "0", "false"),
            # [617] In the primary population now that its state comes from the CCN.
            ("010009", "2021", "HAI_1", "1.1", "1.1", *blank, "0", "true"),
            ("01000F", "2025", "HAI_1", "0.3", "0.3", *blank, "0", "false"),
            ("01001F", "2021", "HAI_1", "0.3", "0.3", *blank, "0", "false"),
            ("011301", "2021", "HAI_1", "0.5", "0.5", *blank, "0", "false"),
            ("070001", "2021", "HAI_1", "0.9", "0.9", "0.5", "1.5", "9.0", "10.0", "1000.0", "", "0", "true"),
            ("070001", "2021", "HAI_2", "Not Available", "", "", "", "0.0", "0.412", "800.0", "13", "0", "true"),
            ("070001", "2021", "HAI_3", "--", "", "", "", "", "", "", "3,13", "0", "true"),
            ("070001", "2021", "HAI_6", "N/A", "", "", "", "", "", "", "12", "0", "true"),
            ("210001", "2021", "HAI_1", "0.8", "0.8", *blank, "0", "false"),
            ("210001", "2021", "HAI_5", "", "", *blank, "1", "false"),
            ("990001", "2021", "HAI_1", "1.0", "1.0", *blank, "0", "false"),
            ("010005", "2019", "HAI_1", "0.6", "0.6", "", "", "", "", "", "", "0", "true"),
            ("010006", "2018", "HAI_1", "0.6", "0.6", "", "", "", "", "", "", "0", "false"),
            ("011301", "2020", "HAI_1", "0.5", "0.5", "", "", "", "", "", "", "0", "false"),
        ]
    )
    # [550] [556] Six types for every spine hospital-window, each once.
    checks["hai_outcomes_six_per_spine_row"] = base.get("hai_outcome_counts") == [
        ("90", "90", "90"),
    ]
    # [560] to [563] Statuses: the baseline-held and staging-held parts are held, not missing; each part keeps its footnote;
    # the SIR's national comparison as published.
    checks["hai_outcome_status_match_expected"] = base.get("hai_outcome_status") == sorted(
        [
            ("070001", "2021", "HAI_1", "aligned", "0", "", "3", ""),
            ("070001", "2021", "HAI_2", "aligned", "0", "Not Available", "", "13"),
            ("070001", "2021", "HAI_3", "aligned", "0", "No Different than National Benchmark", "", ""),
            ("210001", "2021", "HAI_5", "held_in_staging", "0", "", "", ""),
            ("990001", "2021", "HAI_4", "held_in_staging", "1", "", "", ""),
        ]
    )
    checks["hai_outcome_status_counts_match"] = base.get("hai_outcome_status_counts") == [
        ("aligned", "19"),
        ("held_in_staging", "2"),
        ("not_in_source", "69"),
    ]
    # [559] Each registry control from its exact part: the SIR twice (target and earlier-outcome candidates), the counts, the
    # bounds and the benchmark category under C283 and its children; only published values.
    checks["hai_outcome_values_per_control"] = base.get("hai_outcome_values") == [
        ("C269", "15"),
        ("C270", "2"),
        ("C271", "1"),
        ("C274", "1"),
        ("C275", "15"),
        ("C276", "2"),
        ("C277", "1"),
        ("C280", "1"),
        ("C281", "2"),
        ("C282", "2"),
        ("C283", "6"),
        ("C283.benchmark_category", "2"),
        ("C283.lower_limits", "2"),
        ("C283.upper_limits", "2"),
    ]
    checks["hai_outcome_value_rows_match_expected"] = base.get("hai_outcome_value_rows") == sorted(
        [
            ("C270", "070001:2021:HAI_2", "sir", "Not Available", "", ""),
            ("C276", "070001:2021:HAI_2", "sir", "Not Available", "", ""),
            ("C281", "070001:2021:HAI_2", "observed", "0", "0.0", "exclude_primary"),
            ("C282", "070001:2021:HAI_2", "predicted", "0.412", "0.412", "exclude_primary"),
            ("C283", "070001:2021:HAI_2", "ci_lower", "Not Available", "", "exclude_primary"),
            ("C283", "070001:2021:HAI_2", "ci_upper", "Not Available", "", "exclude_primary"),
            ("C283", "070001:2021:HAI_2", "sir_compared_to_national", "Not Available", "", "exclude_primary"),
            ("C283.lower_limits", "070001:2021:HAI_2", "ci_lower", "Not Available", "", ""),
            ("C283.upper_limits", "070001:2021:HAI_2", "ci_upper", "Not Available", "", ""),
            ("C283.benchmark_category", "070001:2021:HAI_2", "sir_compared_to_national", "Not Available", "", ""),
            ("C271", "070001:2021:HAI_3", "sir", "--", "", ""),
            ("C277", "070001:2021:HAI_3", "sir", "--", "", ""),
            ("C283", "070001:2021:HAI_3", "sir_compared_to_national", "No Different than National Benchmark", "", "exclude_primary"),
            ("C283.benchmark_category", "070001:2021:HAI_3", "sir_compared_to_national", "No Different than National Benchmark", "", ""),
        ]
    )
    checks["hai_outcome_seed_matches_registry"] = hai_outcome_seed_matches()
    return checks


def hai_outcome_seed_matches() -> bool:
    """Check the HAI outcome seed against the registry: every control of source main-hai-pdc and each C283 child once per
    type and part, each part named in its control's exact field (a child takes its parent's), decisions as recorded [559]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    controls = {control["id"]: control for control in registry["measure_controls"]}
    parents = set(sources["main-hai-pdc"]["linked_measure_ids"])
    expected = parents | {name for name, control in controls.items() if control.get("parent_id") in parents}
    seed = REPO_ROOT / "dbt/seeds/hai_outcome_measures.csv"
    if not seed.exists():
        return False
    with seed.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if {row["measure_control"] for row in rows} != expected or len({(row["measure_control"], row["hai_type"], row["part"]) for row in rows}) != len(rows):
        return False
    for row in rows:
        control = controls[row["measure_control"]]
        field = (
            control["preserved_controls"].get("current_exact_field") or controls[control["parent_id"]]["preserved_controls"]["current_exact_field"]
        ).lower()
        # A field names one type (HAI_1_SIR) or every type (HAI_1_... through HAI_6_...).
        types = {f"HAI_{n}" for n in range(1, 7)} if " through " in field else {f"HAI_{n}" for n in re.findall(r"hai_(\d)_", field)}
        part = "compared_to_national" if row["part"] == "COMPARED" else row["part"].lower()
        if row["hai_type"] not in types or part not in field:
            return False
        if row["review_decision"] != (control["preserved_controls"].get("current_review_decision") or ""):
            return False
        if row["original_decision"] != (control["preserved_controls"].get("original_decision") or ""):
            return False
    per_control = {name: sum(row["measure_control"] == name for row in rows) for name in expected}
    return all(count == {"C283": 18}.get(name, 6 if name.startswith(("C281", "C282", "C283")) else 1) for name, count in per_control.items())


def program_checks(base: dict[str, Any]) -> dict[str, bool]:
    """Compare the base fixture's D2 program-year models with their expected rows [514] to [520]."""
    checks: dict[str, bool] = {}
    # [514] [515] [516] [518] [524] The revised FY 2021 file wins for 010001, the original keeps 010003; the older TPS files get
    # their reviewed years; Yes/No typed; PSI-90 from either column name; the repeated FY 2024 hospital and the malformed ID are
    # held; the 5-digit TPS ID is padded.
    checks["program_years_match_expected"] = base.get("program_years") == sorted(
        [
            ("hac", "010001", "2023", "4.0", "false", "", "q6q"),
            ("hac", "010001", "2021", "5.7", "false", "", "ha2"),
            ("hac", "010003", "2021", "N/A", "", "", "ha1"),
            ("hac", "010001", "2024", "6.1", "true", "1.02", "ha3"),
            ("vbp", "010001", "2019", "38.0", "", "", "tp0"),
            ("vbp", "010001", "2020", "24.083333333333(23)", "", "", "tp9"),
            ("vbp", "010001", "2025", "23.5", "", "", "tp1"),
            ("vbp", "010003", "2025", "Not Available", "", "", "tp1"),
            ("vbp", "010005", "2019", "30.0", "", "", "tp0"),
        ]
    )
    # [525] Every domain score is kept as published; clinical care and clinical outcomes stay apart.
    checks["vbp_domains_match_expected"] = base.get("vbp_domains") == [
        ("010001", "2019", "", "", "", ""),
        ("010001", "2020", "", "", "", ""),
        ("010001", "2025", "", "12", "5.25", ""),
        ("010003", "2025", "", "", "", ""),
        ("010005", "2019", "8", "", "", ""),
    ]
    # [519] [520] Named fields only; the reviewed odd value and tokens stay text; C288.payment_adjustment has no rows.
    checks["program_values_match_expected"] = base.get("program_values") == sorted(
        [
            ("C285", "010001", "2023", "payment_reduction", "No", ""),
            ("C286", "010001", "2023", "total_hac_score", "4.0", "4.0"),
            ("C285", "010001", "2021", "payment_reduction", "No", ""),
            ("C285", "010003", "2021", "payment_reduction", "N/A", ""),
            ("C285", "010001", "2024", "payment_reduction", "Yes", ""),
            ("C286", "010001", "2021", "total_hac_score", "5.7", "5.7"),
            ("C286", "010003", "2021", "total_hac_score", "N/A", ""),
            ("C286", "010001", "2024", "total_hac_score", "6.1", "6.1"),
            ("C287", "010001", "2025", "unweighted_normalized_safety_domain_score", "10", "10.0"),
            ("C287", "010001", "2025", "weighted_safety_domain_score", "2.5", "2.5"),
            *(
                (control, "010001", year, "total_performance_score", text, number)
                for control in ("C288", "C288.total_performance")
                for year, text, number in (("2019", "38.0", "38.0"), ("2020", "24.083333333333(23)", ""), ("2025", "23.5", "23.5"))
            ),
            ("C288", "010003", "2025", "total_performance_score", "Not Available", ""),
            ("C288.total_performance", "010003", "2025", "total_performance_score", "Not Available", ""),
            ("C288", "010005", "2019", "total_performance_score", "30.0", "30.0"),
            ("C288.total_performance", "010005", "2019", "total_performance_score", "30.0", "30.0"),
        ]
    )
    checks["program_holds_match_expected"] = base.get("program_holds") == [
        ("cms_cc_hac_reduction_program_hospital", "", "", "no_key", "1"),
        ("cms_cc_hac_reduction_program_hospital", "010005", "2024", "repeated_in_file", "2"),
    ]
    return checks


def refuses_condition(action: Any, fragment: str) -> bool:
    """Return whether the action raises the condition map's error with the fragment in its message."""
    try:
        action()
    except mmd_conditions.ConditionError as error:
        return fragment in str(error)
    return False


# POS snapshots end 2018-12-31 (a), 2019-03-31 (b) and 2020-12-31 (c). 010001 changes in b and in c, so three versions;
# 010002 leaves after b, which closes its version at c; 01001F, 011301, 070001 and 990001 miss b, so their c rows open a
# version after a gap; 010005, 010006 and 210001 appear only in c [672] [673].
POS_HISTORY_EXPECTED = [
    ("010001", "2018-12-31", "2019-03-31", "false", "false", "1", "04"),
    ("010001", "2019-03-31", "2020-12-31", "false", "false", "1", ""),
    ("010001", "2020-12-31", "", "true", "false", "1", ""),
    ("010002", "2018-12-31", "2019-03-31", "false", "false", "1", ""),
    ("010002", "2019-03-31", "2020-12-31", "false", "false", "1", ""),
    ("010005", "2020-12-31", "", "true", "false", "1", ""),
    ("010006", "2020-12-31", "", "true", "false", "1", ""),
    ("01001F", "2018-12-31", "2019-03-31", "false", "false", "1", "10"),
    ("01001F", "2020-12-31", "", "true", "true", "1", ""),
    ("011301", "2018-12-31", "2019-03-31", "false", "false", "1", ""),
    ("011301", "2020-12-31", "", "true", "true", "1", ""),
    ("070001", "2018-12-31", "2019-03-31", "false", "false", "1", ""),
    ("070001", "2020-12-31", "", "true", "true", "1", ""),
    ("210001", "2020-12-31", "", "true", "false", "1", ""),
    ("990001", "2018-12-31", "2019-03-31", "false", "false", "1", ""),
    ("990001", "2020-12-31", "", "true", "true", "1", ""),
]
# Owner releases end 2020-07-31 (ow0), 2022-11-30 (ow1), 2025-04-30 (ow2) and 2025-05-31 (o1, which lists no owner with an
# ID and role). 5555555555 is unchanged from ow1 to ow2, so one version over two releases [672]; 9876543210's share
# changes, so two; every owner absent from the next release closes there [673]; individual owners never enter [676].
OWNER_HISTORY_EXPECTED = [
    ("O20000000001", "1111111111", "34", "2020-07-31", "2022-11-30", "false", "false", "1"),
    ("O20000000001", "2222222222", "43", "2020-07-31", "2022-11-30", "false", "false", "1"),
    ("O20000000002", "1111111111", "35", "2025-04-30", "2025-05-31", "false", "false", "1"),
    ("O20000000002", "5555555555", "43", "2022-11-30", "2025-05-31", "false", "false", "2"),
    ("O20000000002", "9876543210", "34", "2022-11-30", "2025-04-30", "false", "false", "1"),
    ("O20000000002", "9876543210", "34", "2025-04-30", "2025-05-31", "false", "false", "1"),
]
# A publisher correction of snapshot b (010001's control type) rewrites exactly that version [677].
POS_REVISED_MIDDLE = tuple(
    replace(item, records=tuple(row + (("gnrl_cntl_type_cd", "2"),) if dict(row)["prvdr_num"] == "010001" else row for row in item.records))
    if item.key == "pb"
    else item
    for item in BASE
)


def fixture_scenarios(workers: int = 1) -> dict[str, bool]:
    """Build every fixture case (in parallel when asked), then run each check in order and return its outcome."""
    checks: dict[str, bool] = {}
    want = expected(BASE)
    jobs: list[tuple[str, tuple[Stored, ...], frozenset[str]]] = [
        ("base", BASE, frozenset()),
        ("base_again", BASE, frozenset()),
        ("reversed_order", tuple(reversed(BASE)), frozenset()),
        ("pos_revised_middle", POS_REVISED_MIDDLE, frozenset()),
    ]
    jobs += [(case, objects, unlabelled) for case, (_test, objects, unlabelled) in FAILING.items()]
    results = build_cases(jobs, workers)
    code, statuses = built(results, "base")
    checks["base_build_passes"] = code == 0 and bool(statuses) and all(status in ("pass", "success") for status in statuses.values())
    base = model_outputs("base")
    checks["copies_match_expected"] = [tuple(row) for row in want["copies"]] == base.get("copies")
    checks["files_match_expected"] = [tuple(row) for row in want["files"]] == base.get("files")
    checks["view_rows_match_distinct_files"] = want["rows"] == base.get("rows")
    # Twins: the quoted, zero-padded, rounded pair agrees and prefers its text file; the fixed-width pair prefers its
    # workbook; the comma pair with a changed value differs and keeps neither [184] to [187].
    checks["twins_match_expected"] = base.get("twins") == [
        ("61", "n2", "tab", "agree", "61"),
        ("d1", "d2", "tab", "agree", "d1"),
        ("d4", "d5", "fixed_width", "not_compared", "d5"),
        ("e1", "e2", "comma", "differ", ""),
        ("g1", "g2", "tab", "agree", "g1"),
        ("h1", "h2", "tab", "agree", "h1"),
        ("j1", "j2", "fixed_width", "not_compared", "j2"),
        ("k1", "k2", "tab", "agree", "k1"),
        ("l1", "l2", "tab", "agree", "l1"),
        ("p1", "p2", "tab", "agree", "p1"),
    ]
    # Not selected: the two held BRZ-016 files (their copies conflict) and the excluded twins [172] [181] [187].
    checks["selection_matches_expected"] = base.get("selection") == [
        ("61", "true", "true", "false", "false"),
        ("83", "true", "true", "false", "false"),
        ("d2", "false", "false", "true", "false"),
        ("d4", "false", "false", "true", "false"),
        ("e1", "false", "false", "true", "false"),
        ("e2", "false", "false", "true", "false"),
        ("g2", "false", "false", "true", "false"),
        ("h2", "false", "false", "true", "false"),
        ("j1", "false", "false", "true", "false"),
        ("k2", "false", "false", "true", "false"),
        ("l2", "false", "false", "true", "false"),
        ("n2", "false", "false", "true", "false"),
        ("p2", "false", "false", "true", "false"),
    ]
    # Sheets of twin-excluded workbooks: covered only by a selected text of the same release whose data rows all match;
    # otherwise kept, except the compared sheet of a pair that differs [267] to [274].
    checks["sheets_match_expected"] = base.get("sheets") == [
        ("d2", "Data", "covered", "d1", "false"),
        ("d2", "Variable Descriptions", "kept", "", "true"),
        ("e2", "Sheet1", "twin_differs", "", "false"),
        ("g2", "Data", "covered", "g1", "false"),
        ("h2", "Sheet1", "covered", "h1", "false"),
        ("k2", "Data", "covered", "k1", "false"),
        ("k2", "Variable Descriptions", "kept", "", "true"),
        ("l2", "CN 2024", "kept", "", "true"),
        ("l2", "FR 2024", "covered", "l1", "false"),
        ("n2", "FY19 NPRM", "kept", "", "true"),
        ("n2", "Variable Descriptions", "kept", "", "true"),
        ("p2", "AHW Data", "covered", "p3", "false"),
        ("p2", "S-3 Data", "covered", "p1", "false"),
    ]
    # [219] [218] A file in two pairs, or a renamed pair from two captures, is refused by the generator.
    stored = generator_objects(BASE)
    checks["generator_refuses_file_in_two_pairs"] = refuses(
        lambda: ipps_file_labels.twins(stored, (*FIXTURE_RENAMED, (sha("d4"), sha("d2")))), "two twin pairs"
    )
    checks["generator_refuses_pair_across_captures"] = refuses(lambda: ipps_file_labels.twins(stored, ((sha("k1"), sha("j2")),)), "one capture")
    # [230] to [235] One row per HAI measurement window, from the latest dated file; conflicts and unreadable rows held.
    checks["hai_windows_match_expected"] = base.get("windows") == sorted(
        [
            ("hospital", "010001", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.4", "a6"),
            ("hospital", "010005", "HAI_1_SIR", "2019-01-01", "2019-12-31", "0.6", "a6"),
            ("hospital", "010006", "HAI_1_SIR", "2018-01-01", "2018-12-31", "0.6", "a6"),
            ("hospital", "010005", "HAI_1_SIR", "2020-04-01", "2021-03-31", "0.7", "a6"),
            ("hospital", "010005", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.6", "a6"),
            ("hospital", "010009", "HAI_1_SIR", "2021-01-01", "2021-12-31", "1.1", "a6"),
            ("hospital", "011301", "HAI_1_SIR", "2020-01-01", "2020-12-31", "0.5", "a6"),
            ("hospital", "011301", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.5", "a6"),
            ("hospital", "01001F", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.3", "a6"),
            ("hospital", "070001", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.9", "a6"),
            ("hospital", "990001", "HAI_1_SIR", "2021-01-01", "2021-12-31", "1.0", "a6"),
            *(("hospital", line.split("|")[0], line.split("|")[1], "2021-01-01", "2021-12-31", line.split("|")[4], "a7") for line in HAI_OUTCOME),
            ("hospital", "210001", "HAI_5_SIR", "2021-01-01", "2021-12-31", "0.7", "a8"),
            ("hospital", "210001", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.8", "a6"),
            ("hospital", "010001", "HAI_1_SIR", "2019-01-01", "2019-12-31", "0.6", "a2"),
            ("hospital", "010001", "HAI_1_SIR", "2025-01-01", "2025-12-31", "1.2", "a3"),
            ("hospital", "010001", "HAI_2_SIR", "2019-01-01", "2019-12-31", "0.7", "a1"),
            ("hospital", "010002", "HAI_1_SIR", "2019-01-01", "2019-12-31", "0.9", "a1"),
            ("hospital", "01000F", "HAI_1_SIR", "2025-01-01", "2025-12-31", "0.3", "a3"),
            ("national", "US", "HAI_1_SIR", "2019-01-01", "2019-12-31", "1.0", "b2"),
            ("state", "AL", "HAI_1_SIR", "2019-01-01", "2019-12-31", "0.8", "b1"),
            ("state", "AL", "HAI_2_SIR", "2019-01-01", "2019-12-31", "0.9", "b1"),
        ]
    )
    checks["hai_holds_match_expected"] = base.get("holds") == [
        ("cms_hai_hospital", "", "", "no_key", "1"),
        ("cms_hai_hospital", "010003", "HAI_1_SIR", "same_date_conflict", "2"),
        ("cms_hai_hospital", "010004", "HAI_1_SIR", "unparsed_date", "1"),
        ("cms_hai_hospital", "990001", "HAI_4_SIR", "same_date_conflict", "2"),
    ]
    # [280] to [292] Hospital rows only, one per CCN and period; padded codes, 5-character counties, typed counts and
    # switches, dates, and the population flags.
    checks["pos_snapshots_match_expected"] = base.get("pos") == [
        (
            "010001",
            "2018-12-31",
            "AL",
            "01073",
            "01360",
            "01",
            "04",
            "00",
            "true",
            "250",
            "240",
            "100.5",
            "true",
            "false",
            "1",
            "1966-07-01",
            "",
            "35233",
            "true",
            "false",
            "false",
            "false",
        ),
        (
            "010001",
            "2019-03-31",
            "AL",
            "01073",
            "",
            "01",
            "",
            "00",
            "true",
            "255",
            "",
            "101.0",
            "true",
            "false",
            "",
            "",
            "",
            "",
            "true",
            "false",
            "false",
            "false",
        ),
        ("010001", "2020-12-31", "AL", "01073", "", "01", "", "00", "true", "260", "", "", "", "", "", "", "", "", "true", "false", "false", "false"),
        ("010002", "2018-12-31", "DC", "", "", "", "", "00", "true", "", "", "", "", "", "", "", "", "20001", "true", "false", "false", "false"),
        ("010002", "2019-03-31", "AL", "", "", "", "", "01", "false", "50", "", "", "", "", "", "", "2019-01-15", "", "true", "false", "false", "false"),
        ("010005", "2020-12-31", "AL", "01089", "", "01", "", "00", "true", "120", "", "", "", "", "", "", "", "", "true", "false", "false", "false"),
        ("010006", "2020-12-31", "AL", "01089", "", "01", "", "00", "true", "80", "", "", "", "", "", "", "", "", "true", "false", "false", "false"),
        ("01001F", "2018-12-31", "AL", "01001", "", "", "10", "00", "true", "100", "", "", "", "", "", "", "", "", "true", "false", "false", "true"),
        ("01001F", "2020-12-31", "AL", "01001", "", "", "", "00", "true", "100", "", "", "", "", "", "", "", "", "true", "false", "false", "true"),
        ("011301", "2018-12-31", "AL", "01003", "", "11", "", "00", "true", "25", "", "", "", "", "", "", "", "", "true", "false", "true", "false"),
        ("011301", "2020-12-31", "AL", "01003", "", "11", "", "00", "true", "25", "", "", "", "", "", "", "", "", "true", "false", "true", "false"),
        ("070001", "2018-12-31", "CT", "09001", "", "", "", "00", "true", "", "", "", "", "", "", "", "", "", "true", "true", "false", "false"),
        ("070001", "2020-12-31", "CT", "09001", "", "01", "", "00", "true", "300", "", "", "", "", "", "", "", "06001", "true", "true", "false", "false"),
        ("210001", "2020-12-31", "MD", "24005", "", "01", "", "00", "true", "200", "", "", "", "", "", "", "", "", "true", "false", "false", "false"),
        ("990001", "2018-12-31", "CN", "", "", "", "", "00", "true", "", "", "", "", "", "", "", "", "", "false", "false", "false", "false"),
        ("990001", "2020-12-31", "CN", "", "", "", "", "00", "true", "", "", "", "", "", "", "", "", "35004", "false", "false", "false", "false"),
    ]
    # [293] to [304] Every row of the selected CMI files, unadjusted CMI beside the transfer-adjusted one; the occupational-mix
    # file in a CMI snapshot is not read.
    checks["cmi_rows_match_expected"] = base.get("cmi_rows") == sorted(
        [
            ("q1", "2020", "2018", "final", "010001", "1.9186", "7072.0", "13568.39", "", "true"),
            ("q1", "2020", "2018", "final", "010005", "1.381", "3140.0", "4336.49", "", "true"),
            ("q2", "2020", "2018", "proposed", "010001", "1.9188", "7042.0", "13511.8844", "", "false"),
            ("q2", "2020", "2018", "proposed", "010007", "1.2", "1000.0", "1200.0", "", "false"),
            ("q3", "2025", "2023", "correction", "010001", "2.0542", "4209.0", "8646.1878", "", "true"),
            ("q4", "2025", "2023", "final", "010001", "2.054", "4209.0", "8645.29", "", "true"),
            ("q4", "2025", "2023", "final", "010005", "1.5456", "1479.0", "2285.89", "", "true"),
            ("q5", "2007", "", "final", "010001", "1.4972", "10147.0", "15192.4568", "1.4815", "true"),
            ("q6", "2021", "", "final", "010001", "1.690319553", "8178.0", "13823.4333", "1.673542371", "true"),
            ("q6", "2021", "", "final", "010005", "1.267981138", "2232.0", "2830.1339", "1.260570928", "true"),
            ("q6", "2021", "", "final", "010006", "1.6", "5000.0", "8000.0", "1.59596", "true"),
            ("q7", "2021", "2019", "final", "010001", "1.9837", "6752.0", "13394.11", "", "true"),
            ("q7", "2021", "2019", "final", "010006", "1.6", "5000.0", "8000.0", "", "true"),
            ("q8", "2027", "2025", "final", "010001", "1.8688", "4524.0", "8454.4376", "", "true"),
            ("r1", "2011", "", "unspecified", "010001", "1.751124", "7913.0", "13856.646", "", "false"),
            ("s1", "2023", "2020", "proposed", "010001", "2.0352", "4818.0", "9805.71", "", "true"),
            ("s1", "2023", "2020", "proposed", "010005", "1.6864", "1950.0", "3288.4", "", "true"),
            ("s1", "2023", "2020", "proposed", "010005", "1.7", "2000.0", "3400.0", "", "true"),
            ("s1", "2023", "2020", "proposed", "010009", "1.2", "500.0", "600.0", "", "true"),
            ("s1", "2023", "2020", "proposed", "070001", "1.5", "1000.0", "1500.0", "", "true"),
            ("s1", "2023", "2020", "proposed", "210001", "1.8", "1000.0", "1800.0", "", "true"),
            ("s2", "2023", "2021", "final", "010001", "2.0375", "4837.0", "9855.42", "", "true"),
            ("s3", "2022", "2019", "final", "010001", "1.9837", "6752.0", "13394.11", "", "true"),
            ("s3", "2022", "2019", "final", "010006", "1.7", "5000.0", "8500.0", "", "true"),
        ]
    )
    # [301] One CMI per CCN and rule year from the best stage's files: a correction over the final file, a final file over
    # the proposed one; two final files that disagree on the CMI or the data year are held.
    checks["cmi_years_match_expected"] = base.get("cmi_years") == sorted(
        [
            ("010001", "2007", "", "final", "1.4972", "q5"),
            ("010001", "2011", "", "unspecified", "1.751124", "r1"),
            ("010001", "2020", "2018", "final", "1.9186", "q1"),
            ("010005", "2020", "2018", "final", "1.381", "q1"),
            ("010005", "2021", "", "final", "1.267981138", "q6"),
            ("010001", "2025", "2023", "correction", "2.0542", "q3"),
            ("010001", "2027", "2025", "final", "1.8688", "q8"),
            ("010001", "2022", "2019", "final", "1.9837", "s3"),
            ("010006", "2022", "2019", "final", "1.7", "s3"),
            ("010001", "2023", "2021", "final", "2.0375", "s2"),
        ]
    )
    checks["cmi_holds_match_expected"] = base.get("cmi_holds") == [
        ("data", "010005", "2020", "values_disagree", "1"),
        ("rule", "010001", "2021", "values_disagree", "2"),
        ("rule", "010006", "2021", "values_disagree", "2"),
    ]
    # [311] [312] One CMI per CCN and data year: the latest rule that carries the data year, then its best stage.
    checks["cmi_data_years_match_expected"] = base.get("cmi_data_years") == sorted(
        [
            ("010001", "2018", "2020", "final", "1.9186", "q1"),
            ("010005", "2018", "2020", "final", "1.381", "q1"),
            ("010001", "2019", "2022", "final", "1.9837", "s3"),
            ("010006", "2019", "2022", "final", "1.7", "s3"),
            ("010001", "2020", "2023", "proposed", "2.0352", "s1"),
            ("010009", "2020", "2023", "proposed", "1.2", "s1"),
            ("070001", "2020", "2023", "proposed", "1.5", "s1"),
            ("210001", "2020", "2023", "proposed", "1.8", "s1"),
            ("010001", "2021", "2023", "final", "2.0375", "s2"),
            ("010001", "2023", "2025", "correction", "2.0542", "q3"),
            ("010001", "2025", "2027", "final", "1.8688", "q8"),
        ]
    )
    # [306] to [316] One row per hospital and calendar-year window, as of the window start: the POS snapshot that ends in the
    # 12 months before the window, the CMI of the fiscal year before it, and the population flags; [612] to [618], [631],
    # [698] and [701] the fills, each with its source. Reviewed row by row.
    # Silver step 7.3: SCD2 history from source dates only [670] to [677].
    checks["pos_history_matches_expected"] = base.get("pos_history") == POS_HISTORY_EXPECTED
    checks["ownership_history_matches_expected"] = base.get("owner_history") == OWNER_HISTORY_EXPECTED
    checks["hgi_history_built"] = bool(base.get("hgi_history")) and base["hgi_history"][0][0] != "0"
    code, statuses = built(results, "pos_revised_middle")
    revised = model_outputs("pos_revised_middle").get("pos_history", [])
    changed = set(revised) ^ set(base.get("pos_history", []))
    checks["pos_correction_rewrites_one_version"] = code == 0 and changed == {
        ("010001", "2019-03-31", "2020-12-31", "false", "false", "1", ""),
        ("010001", "2019-03-31", "2020-12-31", "false", "false", "1", "02"),
    }
    checks["spine_matches_expected"] = base.get("spine") == sorted(
        [
            # [701] The only snapshot ends 36 months after the window starts: nothing is carried; the state comes from the CCN.
            (
                "010006",
                "2018",
                "",
                "AL",
                "",
                "",
                "",
                "",
                "",
                "false",
                "false",
                "false",
                "false",
                "false",
                "true",
                "false",
                "false",
                "false",
                "ccn_state_code",
                "",
                "",
                "",
                "",
                "",
            ),
            (
                "010001",
                "2019",
                "2018-12-31",
                "AL",
                "01073",
                "1.9186",
                "2018",
                "2020",
                "final",
                "true",
                "true",
                "false",
                "false",
                "false",
                "true",
                "false",
                "true",
                "true",
                "pos",
                "pos",
                "pos",
                "01",
                "",
                "",
            ),
            (
                "010001",
                "2021",
                "2020-12-31",
                "AL",
                "01073",
                "2.0352",
                "2020",
                "2023",
                "proposed",
                "true",
                "true",
                "false",
                "false",
                "false",
                "true",
                "false",
                "true",
                "true",
                "pos",
                "pos",
                "pos",
                "01",
                "",
                "",
            ),
            # [613] [614] [701] 49 months after the last snapshot: too far to carry; the state comes from the CCN, no county.
            (
                "010001",
                "2025",
                "",
                "AL",
                "",
                "",
                "",
                "",
                "",
                "false",
                "false",
                "false",
                "false",
                "false",
                "true",
                "false",
                "false",
                "false",
                "ccn_state_code",
                "",
                "",
                "",
                "",
                "",
            ),
            # [616] [698] No county in its snapshot and HUD splits the ZIP: no county, since a majority share is not exact.
            (
                "010002",
                "2019",
                "2018-12-31",
                "DC",
                "",
                "",
                "",
                "",
                "",
                "true",
                "false",
                "false",
                "false",
                "false",
                "true",
                "false",
                "false",
                "false",
                "pos",
                "",
                "pos",
                "",
                "",
                "",
            ),
            # [613] [614] [617] Before the first snapshot: location and classification from the first later one; a CMI, so primary.
            (
                "010005",
                "2019",
                "",
                "AL",
                "01089",
                "1.381",
                "2018",
                "2020",
                "final",
                "false",
                "true",
                "false",
                "false",
                "false",
                "true",
                "false",
                "true",
                "true",
                "pos_other_snapshot",
                "pos_other_snapshot",
                "pos_other_snapshot",
                "01",
                "",
                "",
            ),
            (
                "010005",
                "2021",
                "2020-12-31",
                "AL",
                "01089",
                "",
                "",
                "",
                "",
                "true",
                "false",
                "true",
                "false",
                "false",
                "true",
                "false",
                "false",
                "false",
                "pos",
                "pos",
                "pos",
                "01",
                "",
                "",
            ),
            # [615] [617] In no POS file and no enrollment: the state from the CCN's SSA code; a CMI, so primary.
            (
                "010009",
                "2021",
                "",
                "AL",
                "",
                "1.2",
                "2020",
                "2023",
                "proposed",
                "false",
                "true",
                "false",
                "false",
                "false",
                "true",
                "false",
                "true",
                "true",
                "ccn_state_code",
                "",
                "",
                "",
                "",
                "",
            ),
            (
                "01000F",
                "2025",
                "",
                "AL",
                "",
                "",
                "",
                "",
                "",
                "false",
                "false",
                "false",
                "false",
                "true",
                "true",
                "false",
                "false",
                "false",
                "ccn_state_code",
                "",
                "",
                "",
                "",
                "",
            ),
            (
                "01001F",
                "2021",
                "2020-12-31",
                "AL",
                "01001",
                "",
                "",
                "",
                "",
                "true",
                "false",
                "false",
                "false",
                "true",
                "true",
                "false",
                "false",
                "false",
                "pos",
                "pos",
                "pos",
                "",
                "",
                "",
            ),
            # [614] Between two snapshots that agree: classification filled.
            (
                "011301",
                "2020",
                "",
                "AL",
                "01003",
                "",
                "",
                "",
                "",
                "false",
                "false",
                "false",
                "true",
                "false",
                "true",
                "false",
                "false",
                "true",
                "pos_other_snapshot",
                "pos_other_snapshot",
                "pos_other_snapshot",
                "11",
                "",
                "",
            ),
            (
                "011301",
                "2021",
                "2020-12-31",
                "AL",
                "01003",
                "",
                "",
                "",
                "",
                "true",
                "false",
                "false",
                "true",
                "false",
                "true",
                "false",
                "false",
                "true",
                "pos",
                "pos",
                "pos",
                "11",
                "",
                "",
            ),
            # [631] Connecticut: the planning region from its ZIP.
            (
                "070001",
                "2021",
                "2020-12-31",
                "CT",
                "09001",
                "1.5",
                "2020",
                "2023",
                "proposed",
                "true",
                "true",
                "false",
                "false",
                "false",
                "true",
                "true",
                "true",
                "true",
                "pos",
                "pos",
                "pos",
                "01",
                "09110",
                "hud_zip_single_county",
            ),
            (
                "210001",
                "2021",
                "2020-12-31",
                "MD",
                "24005",
                "1.8",
                "2020",
                "2023",
                "proposed",
                "true",
                "true",
                "false",
                "false",
                "false",
                "true",
                "false",
                "false",
                "true",
                "pos",
                "pos",
                "pos",
                "01",
                "",
                "",
            ),
            # [616] A ZIP whose HUD rows are in another state: no county.
            (
                "990001",
                "2021",
                "2020-12-31",
                "CN",
                "",
                "",
                "",
                "",
                "",
                "true",
                "false",
                "false",
                "false",
                "false",
                "false",
                "false",
                "false",
                "false",
                "pos",
                "",
                "pos",
                "",
                "",
                "",
            ),
        ]
    )
    # [318] to [322] Care Compare windows as for HAI: the latest release wins, conflicts and unparsed dates are held.
    checks["timely_windows_match_expected"] = base.get("timely") == [
        ("010001", "EDV", "2020-01-01", "2020-12-31", "Low", "", "z6"),
        ("010001", "OP_18b", "2018-01-01", "2018-12-31", "130", "", "z6"),
        ("010001", "OP_18b", "2020-01-01", "2020-12-31", "145", "", "z6"),
        ("010001", "OP_18b", "2022-01-01", "2022-12-31", "152", "310", "w2"),
        ("010002", "EDV", "2022-01-01", "2022-12-31", "high", "", "w1"),
    ]
    checks["maternal_windows_match_expected"] = base.get("maternal") == [
        ("010001", "PC_02", "2023-01-01", "2023-12-31", "30", "100", "w4"),
        ("010001", "SM_7", "2023-01-01", "2023-12-31", "Yes", "", "w4"),
    ]
    checks["hcahps_windows_match_expected"] = base.get("hcahps") == [
        ("010001", "H_COMP_1_A_P", "2022-01-01", "2022-12-31", "80", "507", "21", "", "w5"),
        ("010001", "H_STAR_RATING", "2022-01-01", "2022-12-31", "Not Applicable", "507", "21", "4", "w5"),
    ]
    checks["cc_holds_match_expected"] = base.get("cc_holds") == [
        ("cms_cc_timely_and_effective_care_hospital", "010001", "SEP_1", "same_date_conflict", "2"),
        ("cms_cc_timely_and_effective_care_hospital", "010003", "SEP_1", "unparsed_date", "1"),
        ("cms_cc_timely_and_effective_care_hospital", "010005", "OP_18b", "same_date_conflict", "2"),
    ]
    # [323] [325] [326] [327] Registry controls get exactly their named measure; numbers only for plain numbers.
    checks["registry_windows_match_expected"] = base.get("registry") == [
        ("C119", "010001", "2022-01-01", "80", "80.0", ""),
        ("C139", "010001", "2022-01-01", "21", "21.0", ""),
        ("C140", "010001", "2022-01-01", "507", "507.0", ""),
        ("C141", "010001", "2020-01-01", "Low", "", ""),
        ("C141", "010002", "2022-01-01", "high", "", ""),
        ("C143", "010001", "2018-01-01", "130", "130.0", ""),
        ("C143", "010001", "2020-01-01", "145", "145.0", ""),
        ("C143", "010001", "2022-01-01", "152", "152.0", ""),
        ("C167", "010001", "2023-01-01", "Yes", "", ""),
        ("C168", "010001", "2023-01-01", "30", "30.0", ""),
    ]
    # [328] [329] One Hospital General Information row per CCN and file; two files on one release date are both kept.
    checks["hgi_releases_match_expected"] = base.get("hgi") == [
        ("010001", "2020-07-01", "1", "Acute Care Hospitals", "", "2", "2", "", "z8"),
        ("010001", "2024-01-31", "2", "Acute Care Hospitals", "true", "3", "3", "", "u5"),
        ("010001", "2024-01-31", "2", "Acute Care Hospitals", "true", "4", "4", "", "u1"),
        ("010005", "2020-10-01", "2", "Acute Care Hospitals", "", "3", "3", "", "z9"),
        ("010005", "2020-10-01", "2", "Acute Care Hospitals", "", "5", "5", "", "r6"),
        ("010005", "2024-01-31", "2", "Critical Access Hospitals", "false", "", "Not Available", "16", "u1"),
    ]
    # [331] to [338] One row per cost report: fiscal year by the period start, inclusive days, full years by the anniversary,
    # reports per CCN and fiscal year, ZIP and urban/rural as approved, amounts typed.
    checks["cost_reports_match_expected"] = base.get("cost_reports") == [
        ("090001", "010001", "2022", "2022", "2021-10-01", "2022-09-30", "365", "true", "1", "", "", "", "", ""),
        ("090002", "010005", "2022", "2022", "2021-11-15", "2022-11-14", "365", "true", "1", "", "", "", "", ""),
        ("100001", "010001", "2023", "2023", "2022-10-01", "2023-09-30", "365", "true", "2", "35233", "urban", "250.0", "0.25", "-10.0"),
        ("100002", "010002", "2023", "2023", "2023-01-01", "2023-06-30", "181", "false", "1", "35233", "rural", "0.0", "", ""),
        ("100003", "010001", "2023", "2023", "2023-07-01", "2023-09-30", "92", "false", "2", "", "", "", "", ""),
    ]
    # [339] [340] Registry measures by the seed's columns and rule; a zero or missing denominator gives null.
    checks["cost_measures_match_expected"] = base.get("cost_measures") == [
        ("100001", "C001", "250.0", ""),
        ("100001", "C002", "", "2"),
        ("100001", "C003", "", "1"),
        ("100001", "C023", "0.5", ""),
        ("100001", "C024", "0.1", ""),
        ("100001", "C027", "1500.5", ""),
        ("100001", "C028", "6.002", ""),
        ("100001", "C035", "4380000.0", ""),
        ("100001", "C037", "73000.0", ""),
        ("100001", "C038", "14600.0", ""),
        ("100001", "C039", "200.0", ""),
        ("100001", "C040", "0.8", ""),
        ("100001", "C041", "58.4", ""),
        ("100001", "C042", "5.0", ""),
        ("100001", "C045", "0.124", ""),
        ("100001", "C046", "2.0", ""),
        ("100001", "C047", "0.5", ""),
        ("100001", "C048", "30.0", ""),
        ("100001", "C049", "0.0125", ""),
        ("100001", "C050", "0.02", ""),
        ("100001", "C051", "-10.0", ""),
        ("100001", "C052", "0.1", ""),
        ("100001", "C053", "0.5", ""),
        ("100001", "C054", "0.01", ""),
        ("100001", "C055", "0.25", ""),
        ("100001", "C057", "30000.0", ""),
        ("100002", "C001", "0.0", ""),
        ("100002", "C027", "100.0", ""),
        ("100002", "C028", "", ""),
        ("100002", "C046", "", ""),
    ]
    # [342] to [352] Impact files by exact header: SAS dots kept as text with no number, blanks null, leading spaces trimmed,
    # a footer row dropped, a quoted comma kept in one field, no wage index before FY 2011, revised correction-notice columns,
    # lost leading zeros restored; excluded families and description sheets not read.
    checks["impact_values_match_expected"] = base.get("impact_values") == [
        ("2009", "unspecified", "", "010001", "average_daily_census", "242", "242.0"),
        ("2009", "unspecified", "", "010001", "beds", "370", "370.0"),
        ("2009", "unspecified", "", "010001", "capital_ime_factor", "0.028", "0.028"),
        ("2009", "unspecified", "", "010001", "dsh_patient_percentage", "0.1274", "0.1274"),
        ("2009", "unspecified", "", "010001", "geographic_labor_market_area", "20020", ""),
        ("2009", "unspecified", "", "010001", "medicare_bills", "10216", "10216.0"),
        ("2009", "unspecified", "", "010001", "medicare_percentage", "27.3%", "0.273"),
        ("2009", "unspecified", "", "010001", "operating_ime_factor", "0.05944", "0.05944"),
        ("2009", "unspecified", "", "010001", "resident_to_bed_ratio", "0.28515", "0.28515"),
        ("2009", "unspecified", "", "010001", "urgeo", "OURBAN", ""),
        ("2015", "correction+final", "impact_puf15", "010001", "beds", "50", "50.0"),
        ("2015", "correction+final", "impact_puf15", "010001", "medicare_bills", "500", "500.0"),
        ("2015", "correction+final", "impact_puf15", "010001", "wage_index", "0.75", "0.75"),
        ("2015", "correction+final", "impact_puf15-Sept CN", "010001", "beds", "50", "50.0"),
        ("2015", "correction+final", "impact_puf15-Sept CN", "010001", "medicare_bills", "510", "510.0"),
        ("2015", "correction+final", "impact_puf15-Sept CN", "010001", "wage_index", "0.76", "0.76"),
        ("2025", "final", "", "010010", "beds", "20", "20.0"),
        ("2026", "final", "", "010001", "average_daily_census", "300", "300.0"),
        ("2026", "final", "", "010001", "beds", "400", "400.0"),
        ("2026", "final", "", "010001", "capital_ime_factor", "0.05731", "0.05731"),
        ("2026", "final", "", "010001", "dsh_patient_percentage", "0.2752", "0.2752"),
        ("2026", "final", "", "010001", "geographic_labor_market_area", "20020", ""),
        ("2026", "final", "", "010001", "medicaid_percentage", "0.013", "0.013"),
        ("2026", "final", "", "010001", "medicare_bills", "3952", "3952.0"),
        ("2026", "final", "", "010001", "medicare_percentage", "0.187", "0.187"),
        ("2026", "final", "", "010001", "operating_ime_factor", "0.0298", "0.0298"),
        ("2026", "final", "", "010001", "resident_to_bed_ratio", "0.0655", "0.0655"),
        ("2026", "final", "", "010001", "urgeo", "OURBAN", ""),
        ("2026", "final", "", "010001", "wage_index", "0.9049", "0.9049"),
        ("2026", "final", "", "010005", "average_daily_census", ".", ""),
        ("2026", "final", "", "010005", "beds", "100", "100.0"),
        ("2026", "final", "", "010005", "capital_ime_factor", "0", "0.0"),
        ("2026", "final", "", "010005", "dsh_patient_percentage", ".", ""),
        ("2026", "final", "", "010005", "geographic_labor_market_area", "01", ""),
        ("2026", "final", "", "010005", "medicaid_percentage", "0.02", "0.02"),
        ("2026", "final", "", "010005", "medicare_bills", "1200", "1200.0"),
        ("2026", "final", "", "010005", "medicare_percentage", "", ""),
        ("2026", "final", "", "010005", "operating_ime_factor", "0", "0.0"),
        ("2026", "final", "", "010005", "resident_to_bed_ratio", "", ""),
        ("2026", "final", "", "010005", "urgeo", "RURAL", ""),
        ("2026", "final", "", "010005", "wage_index", "0.8", "0.8"),
    ]
    checks["impact_holds_match_expected"] = base.get("impact_holds") == [("2025", "", "010009", "repeated_in_file", "2")]
    # [353] Registry measures by the seed's field and rule; a missing or dotted value gives no measure row.
    checks["impact_measures_match_expected"] = base.get("impact_measures") == [
        ("010001", "C001", "beds", "400.0", ""),
        ("010001", "C006", "resident_to_bed_ratio", "0.0655", ""),
        ("010001", "C021", "geographic_labor_market_area", "", "20020"),
        ("010001", "C021", "urgeo", "", "OURBAN"),
        ("010001", "C022", "dsh_patient_percentage", "0.2752", ""),
        ("010001", "C023", "medicare_percentage", "0.187", ""),
        ("010001", "C024", "medicaid_percentage", "0.013", ""),
        ("010001", "C025", "wage_index", "0.9049", ""),
        ("010001", "C026", "capital_ime_factor", "0.05731", ""),
        ("010001", "C026", "operating_ime_factor", "0.0298", ""),
        ("010001", "C038", "medicare_bills", "3952.0", ""),
        ("010001", "C040", "average_daily_census", "0.75", ""),
        ("010005", "C001", "beds", "100.0", ""),
        ("010005", "C021", "geographic_labor_market_area", "", "01"),
        ("010005", "C021", "urgeo", "", "RURAL"),
        ("010005", "C024", "medicaid_percentage", "0.02", ""),
        ("010005", "C025", "wage_index", "0.8", ""),
        ("010005", "C026", "capital_ime_factor", "0.0", ""),
        ("010005", "C026", "operating_ime_factor", "0.0", ""),
        ("010005", "C038", "medicare_bills", "1200.0", ""),
    ]
    # [355] to [359] Medicare inpatient: the data and release years from the file name in any case; suppressed values null.
    checks["mup_providers_match_expected"] = base.get("mup_providers") == [
        ("2023", "2025", "010001", "1000.0", "1500.0", "", "0.4"),
        ("2023", "2025", "010005", "", "12.0", "", ""),
        ("2024", "2026", "010001", "900.0", "1350.0", "", ""),
    ]
    checks["mup_drg_match_expected"] = base.get("mup_drg") == [
        ("2023", "010001", "470", "100.0"),
        ("2023", "010001", "871", "60.0"),
        ("2023", "010001", "872", "30.0"),
        ("2023", "010005", "871", "12.0"),
    ]
    # [360] to [363] Registry measures by the seed: C084 per race field, a suppressed count gives no row, a missing
    # denominator gives null, the sepsis share from DRG 870 to 872, held controls give no rows.
    checks["mup_measures_match_expected"] = base.get("mup_measures") == [
        ("010001", "C038", "tot_dschrgs", "1500.0"),
        ("010001", "C042", "tot_days", "5.0"),
        ("010001", "C076", "bene_avg_risk_scre", "1.8"),
        ("010001", "C077", "bene_avg_age", "74.5"),
        ("010001", "C078", "bene_age_lt_65_cnt", "0.1"),
        ("010001", "C079", "bene_age_65_74_cnt", "0.4"),
        ("010001", "C080", "bene_age_75_84_cnt", "0.3"),
        ("010001", "C081", "bene_age_gt_84_cnt", "0.2"),
        ("010001", "C082", "bene_feml_cnt", "0.55"),
        ("010001", "C083", "bene_dual_cnt", "0.25"),
        ("010001", "C084", "bene_race_api_cnt", "0.03"),
        ("010001", "C084", "bene_race_black_cnt", "0.2"),
        ("010001", "C084", "bene_race_hspnc_cnt", "0.05"),
        ("010001", "C084", "bene_race_othr_cnt", "0.02"),
        ("010001", "C084", "bene_race_wht_cnt", "0.7"),
        ("010001", "C085", "tot_benes", "1000.0"),
        ("010001", "C086", "tot_dschrgs", "1.5"),
        ("010001", "C087", "tot_dschrgs", "1500.0"),
        ("010001", "C088", "tot_cvrd_days", "7400.0"),
        ("010001", "C089", "bene_cc_ph_diabetes_v2_pct", "0.35"),
        ("010001", "C090", "bene_cc_ph_ckd_v2_pct", "0.4"),
        ("010001", "C109", "bene_cc_bh_depress_v1_pct", "0.3"),
        ("010001", "C117", "tot_dschrgs", "0.06"),
        ("010005", "C038", "tot_dschrgs", "12.0"),
        ("010005", "C086", "tot_dschrgs", ""),
        ("010005", "C087", "tot_dschrgs", "12.0"),
        ("010005", "C117", "tot_dschrgs", "1.0"),
    ]
    # [365] to [372] Ownership: each file dated by its recorded period; owner flags null before the April 2025 layout and for
    # a blank; one-digit dates; a lost leading zero padded, a unit CCN kept and a value that is not a CCN nulled; a
    # change of ownership kept once per release under one event key.
    checks["owners_match_expected"] = base.get("owners") == [
        ("o2", "2022-11-30", "O20000000002", "5555555555", "43", "2019-11-30", "", "false", "", "", "", "true"),
        ("o2", "2022-11-30", "O20000000002", "9876543210", "34", "2020-03-07", "100.0", "false", "", "", "", "true"),
        ("o3", "2025-04-30", "O20000000002", "1111111111", "35", "2025-04-15", "37.5", "true", "false", "false", "", ""),
        ("o3", "2025-04-30", "O20000000002", "5555555555", "43", "", "", "true", "", "", "", ""),
        ("o3", "2025-04-30", "O20000000002", "9876543210", "34", "2020-03-07", "62.5", "true", "true", "false", "true", "true"),
        ("r8", "2020-07-31", "O20000000001", "1111111111", "34", "", "60.0", "true", "true", "false", "", ""),
        ("r8", "2020-07-31", "O20000000001", "2222222222", "43", "", "", "true", "false", "true", "", ""),
        ("u3", "2025-05-31", "O20000000001", "", "", "", "", "true", "false", "", "", ""),
    ]
    checks["enrollments_match_expected"] = base.get("enrollments") == [
        ("u2", "2024-01-31", "O20000000001", "010001", "010001", "", "", "", ""),
        ("v1", "2022-11-30", "O20000000002", "013025", "13025", "P", "1990-01-05", "true", "false"),
        ("v1", "2022-11-30", "O20000000003", "01T001", "01T001", "", "", "false", ""),
        ("v1", "2022-11-30", "O20000000010", "010001", "01000101", "", "", "", ""),
        ("v1", "2022-11-30", "O20000000011", "010001", "1000101", "", "", "", ""),
        ("v1", "2022-11-30", "O20000000012", "010001", "01S001A", "", "", "", ""),
        ("v1", "2022-11-30", "O20000000013", "", "01000101", "", "", "", ""),
        ("v1", "2022-11-30", "O20000000014", "", "78A005BP", "", "", "", ""),
    ]
    # [619] [620] The route of each mapped CCN.
    checks["enrollment_ccn_sources_match_expected"] = base.get("enrollment_ccn_sources") == [
        ("O20000000010", "010001", "location_suffix"),
        ("O20000000011", "010001", "leading_zero"),
        ("O20000000012", "010001", "unit_parent"),
        ("O20000000013", "", ""),
        ("O20000000014", "", ""),
    ]
    checks["chow_match_expected"] = base.get("chow") == [
        ("u4", "2023-12-31", "010001", "010001", "010001", "", "2023-01-01", "||2023-01-01|"),
        ("y1", "2022-03-31", "", "10002900", "100029", "CH", "2021-03-01", "O20000000004|O20000000005|2021-03-01|CH"),
        ("y2", "2022-09-30", "", "10002900", "100029", "CH", "2021-03-01", "O20000000004|O20000000005|2021-03-01|CH"),
        ("y2", "2022-09-30", "013025", "13025", "01T001", "AM", "2022-07-01", "O20000000006|O20000000007|2022-07-01|AM"),
    ]
    checks["ownership_generator_refuses_file_without_period"] = refuses_period(
        lambda: ownership_release_periods.rows_for(
            [{"table": "cms_hospital_owners", "sha256": sha("o9"), "release_id": "missing", "file_name": "organisation_owners.csv"}], {}
        ),
        "no recorded period",
    )
    checks["ownership_generator_refuses_two_periods"] = refuses_period(
        lambda: ownership_release_periods.receipt_period(
            {
                "snapshot_id": "two",
                "measurement_periods": [
                    {"source_basis": "publisher_stated", "start_date": "2022-01-01", "end_date": "2022-01-31"},
                    {"source_basis": "publisher_stated", "start_date": "2022-02-01", "end_date": "2022-02-28"},
                ],
            }
        ),
        "more than one",
    )
    # [375] to [383] HHS and ONC: a suppressed count and a negative value null and listed, a corrected week kept, a hospital without a CCN kept by
    # its key; the blank criterion null, M/D/YYYY dates, no telephone column; the older attestations typed apart.
    # [681] [682] Only the reviewed facility gets a CCN.
    checks["hhs_sources_match_expected"] = base.get("hhs_sources") == [("3f3f3f", "", ""), ("ee04ed", "190319", "reviewed_match")]
    checks["hhs_match_expected"] = base.get("hhs") == [
        ("010001", "2019-12-29", "010001", "", "", "", "", "", "", ""),
        ("010001", "2020-01-05", "010001", "", "", "", "", "", "", ""),
        ("010001", "2020-01-12", "010001", "", "", "", "", "", "", ""),
        (
            "010001",
            "2021-01-03",
            "010001",
            "false",
            "250.5",
            "",
            "180.0",
            "7.0",
            "inpatient_beds_used_covid_7_day_avg|previous_day_admission_adult_covid_confirmed_50_59_7_day_sum",
            "staffed_pediatric_icu_bed_occupancy_7_day_avg",
        ),
        ("010001", "2021-01-10", "010001", "true", "251.0", "", "", "", "", ""),
        ("010005", "2020-01-05", "010005", "", "", "", "", "", "", ""),
        ("010005", "2020-01-05", "010005", "", "", "", "", "", "", ""),
        ("3f3f3f", "2021-01-03", "", "", "40.0", "", "", "", "", ""),
        ("ee04ed", "2021-01-03", "190319", "", "30.0", "", "", "", "", ""),
    ]
    checks["onc_chpl_match_expected"] = base.get("onc_chpl") == [
        ("010001", "true", "2023-01-01", "2023-12-31", "2023", "15.04.04.1234.Epic.AM.01.1.220101", "Fixture Developer A"),
        ("010005", "", "2024-07-01", "2024-09-30", "2024", "15.04.04.2345.Cern.01.01.1.220202", "Fixture Developer B"),
    ]
    checks["onc_chpl_has_no_telephone"] = base.get("phone_columns") == [("0",)]
    checks["onc_attestations_match_expected"] = base.get("onc_attestations") == [("010001", "2014", "7", "2014", "Fixture Developer A")]
    checks["occmix_matches_expected"] = base.get("occmix") == [
        ("010002", "2021-12-25", "2022-12-24", "false", "0.0", "", "", "", ""),
        ("010003", "2022-01-01", "2022-12-31", "false", "", "", "", "", ""),
        ("010004", "2019-01-01", "2019-12-31", "false", "80.0", "20.0", "0.8", "0.1", "0.05"),
        ("010006", "2022-01-01", "2022-12-31", "false", "100.0", "", "", "", ""),
        ("010006", "2022-01-01", "2022-12-31", "false", "100.0", "", "", "", ""),
        ("010008", "2023-01-01", "2022-12-31", "false", "100.0", "", "", "", ""),
        ("010009", "NULL", "2022-12-31", "false", "100.0", "", "", "", ""),
        ("01014F", "2022-01-01", "2022-12-31", "false", "100.0", "12.0", "0.6667", "0.1333", "0.1667"),
        ("01014F", "2022-01-01", "2022-12-31", "true", "100.0", "", "", "", ""),
        ("10005", "2016-01-01", "2016-12-31", "false", "100.0", "30.0", "1.0", "0.0", "0.0"),
        ("BAD", "2022-01-01", "2022-12-31", "false", "100.0", "", "", "", ""),
    ]
    checks["occmix_measures_match_expected"] = base.get("occmix_measures") == [
        ("C030", "11", "4"),
        ("C031", "11", "3"),
        ("C032", "11", "3"),
        ("C033", "11", "3"),
        ("C034", "11", "3"),
        ("C043", "11", "0"),
    ]
    checks["occmix_seed_matches_registry"] = occmix_seed_matches()
    checks["occmix_holds_match_expected"] = base.get("occmix_holds") == [
        ("deleted_survey_record", "1"),
        ("duplicate_provider_period", "2"),
        ("invalid_provider_id", "1"),
        ("invalid_survey_period", "2"),
    ]
    checks.update(geography_checks(base))
    checks.update(county_context_checks(base))
    checks.update(income_labor_checks(base))
    checks.update(county_health_checks(base))
    checks.update(shortage_checks(base))
    checks.update(validation_checks(base))
    checks.update(program_checks(base))
    checks.update(outcome_checks(base))
    checks.update(care_compare_checks(base))
    checks.update(hospital_measure_checks(base))
    checks.update(operations_measure_checks(base))
    checks.update(validation_aligned_checks(base))
    checks.update(county_measure_checks(base))
    checks.update(al4b_context_checks(base))
    code, _ = built(results, "base_again")
    checks["rebuild_identical"] = code == 0 and "error" not in base and model_outputs("base_again") == base
    code, _ = built(results, "reversed_order")
    checks["load_order_independent"] = code == 0 and "error" not in base and model_outputs("reversed_order") == base
    for case, (test, _objects, _unlabelled) in FAILING.items():
        result = results[case]
        checks[f"{case}_fails_{test}"] = not isinstance(result, BaseException) and result[0] != 0 and result[1].get(test) == "fail"
    # The base outputs' hash lets a serial and a parallel run on the same code be compared [690].
    FIXTURE_EVIDENCE["base_outputs_sha256"] = hashlib.sha256(json.dumps(base, sort_keys=True, default=str).encode()).hexdigest()
    FIXTURE_EVIDENCE["case_errors"] = {case: str(result) for case, result in sorted(results.items()) if isinstance(result, BaseException)}
    return checks


FIXTURE_EVIDENCE: dict[str, Any] = {}


REAL_ATTACH = """.output /dev/null
SET autoinstall_known_extensions = false;
LOAD iceberg;
LOAD httpfs;
LOAD aws;
CREATE SECRET lakehouse_s3 (TYPE s3, PROVIDER credential_chain, CHAIN 'config', PROFILE getenv('AWS_PROFILE'), REGION getenv('AWS_REGION'));
CREATE SECRET lakehouse_catalog (
    TYPE iceberg,
    CLIENT_ID getenv('DBT_ENV_SECRET_POLARIS_CLIENT_ID'),
    CLIENT_SECRET getenv('DBT_ENV_SECRET_POLARIS_CLIENT_SECRET'),
    OAUTH2_SERVER_URI 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL'
);
ATTACH 'hai_lakehouse' AS lakehouse (
    TYPE iceberg, ENDPOINT 'http://polaris:8181/api/catalog', SECRET lakehouse_catalog, ACCESS_DELEGATION_MODE 'none', READ_ONLY
);
.output stdout
"""


def registry_seed_matches() -> bool:
    """Check the registry measure seed against the source registry: every control of S13 to S16 once, with the named ID [323] [324]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    families = {family["id"]: family for family in registry["source_families"]}
    sources = {source["source_id"]: source for source in registry["sources"]}
    fields = {control["id"]: control["preserved_controls"].get("current_exact_field", "") for control in registry["measure_controls"]}
    expected = {
        control
        for family in ("S13", "S14", "S15", "S16")
        for source_id in families[family]["audit_source_ids"]
        if source_id in sources
        for control in sources[source_id]["linked_measure_ids"]
    }
    with (REPO_ROOT / "dbt/seeds/registry_measure_sources.csv").open(newline="") as handle:
        seed = list(csv.DictReader(handle))
    named = all(
        not row["source_measure_id"] or row["source_measure_id"] in fields[row["measure_control"]] or row["measure_control"] in ("C139", "C140") for row in seed
    )
    return named and sorted(row["measure_control"] for row in seed) == sorted(expected)


def cost_seed_matches() -> bool:
    """Check that the cost-report measure seed covers exactly the registry's S01 controls [340]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    with (REPO_ROOT / "dbt/seeds/cost_report_measures.csv").open(newline="") as handle:
        seed = sorted(row["measure_control"] for row in csv.DictReader(handle))
    return seed == sorted(sources["CMS_HCRIS_PUF"]["linked_measure_ids"])


def impact_seed_matches() -> bool:
    """Check that the impact measure seed covers exactly the registry's CMS_IPPS controls [353]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    with (REPO_ROOT / "dbt/seeds/impact_measures.csv").open(newline="") as handle:
        seed = sorted({row["measure_control"] for row in csv.DictReader(handle)})
    return seed == sorted(sources["CMS_IPPS"]["linked_measure_ids"])


def hhs_onc_seed_matches(field_names: dict[str, str]) -> bool:
    """Check the HHS and ONC measure seed against the registry [378] [384].

    Every control HHS_CAPACITY, HHS and ONC_PI link is in the seed once, by itself or by all its children; each child's
    columns are its exact field names, an API field name the stored columns.json resolves to its CSV header, then the
    bronze column rule.
    """
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    controls = {control["id"]: control for control in registry["measure_controls"]}
    with (REPO_ROOT / "dbt/seeds/hhs_onc_measures.csv").open(newline="") as handle:
        seed = list(csv.DictReader(handle))
    linked = {*sources["HHS_CAPACITY"]["linked_measure_ids"], *sources["HHS"]["linked_measure_ids"], *sources["ONC_PI"]["linked_measure_ids"]}
    if {row["parent_control"] or row["measure_control"] for row in seed} != linked:
        return False
    children = {control_id for control_id, control in controls.items() if control.get("parent_id") in linked}
    if {row["measure_control"] for row in seed if row["parent_control"]} != children:
        return False
    columns = set(field_names.values())
    for row in seed:
        if not row["parent_control"]:
            continue
        names = [name.strip(" .,;") for name in controls[row["measure_control"]]["preserved_controls"]["current_exact_field"].split("/")]
        derived = [field_names.get(name, name) for name in names]
        if row["fields"].split() != derived or not set(derived) <= columns:
            return False
    return True


def occmix_seed_matches() -> bool:
    """Check every S03 definition and decision against the registry, with C043 unmapped."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    controls = {control["id"]: control["preserved_controls"] for control in registry["measure_controls"]}
    with (REPO_ROOT / "dbt/seeds/occmix_measures.csv").open(newline="") as handle:
        seed = list(csv.DictReader(handle))
    linked = {*sources["OM"]["linked_measure_ids"], *sources["CMS_OCCMIX"]["linked_measure_ids"]}
    return sorted(row["measure_control"] for row in seed) == sorted(linked) and all(
        row["definition"] == controls[row["measure_control"]]["current_exact_field"]
        and row["review_decision"] == controls[row["measure_control"]]["current_review_decision"]
        and (row["measure_control"] != "C043" or (not row["value_column"] and row["source_status"] == "unavailable_source_definition"))
        for row in seed
    )


def ownership_seed_matches() -> bool:
    """Check that the ownership measure seed covers exactly the registry's controls of CMS_OWNERS, ENROLL and CMS_CHOW [373]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    with (REPO_ROOT / "dbt/seeds/ownership_measures.csv").open(newline="") as handle:
        seed = sorted(row["measure_control"] for row in csv.DictReader(handle))
    expected = {*sources["CMS_OWNERS"]["linked_measure_ids"], *sources["ENROLL"]["linked_measure_ids"], *sources["CMS_CHOW"]["linked_measure_ids"]}
    return seed == sorted(expected)


def geography_seed_matches() -> bool:
    """Check the geography measure seed against the source registry: every control of S21, S22, S35, S36 and S38 once, with its family and decision [417]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    families = {source: family["id"] for family in registry["source_families"] for source in family["audit_source_ids"]}
    wanted = {"S21", "S22", "S35", "S36", "S38"}
    expected_rows = sorted(
        (control["id"], family, control["preserved_controls"]["current_review_decision"])
        for control in registry["measure_controls"]
        for family in sorted({families[source] for source in control["source_ids"] if source in families} & wanted)
    )
    seed = REPO_ROOT / "dbt/seeds/geography_measures.csv"
    if not seed.exists():
        return False
    with seed.open(newline="") as handle:
        seeded = sorted((row["measure_control"], row["family"], row["review_decision"]) for row in csv.DictReader(handle))
    return bool(expected_rows) and seeded == expected_rows


def acs_svi_seed_matches() -> bool:
    """Check the ACS and SVI measure seed against the registry (every control of S18 and S19, with its decision) and its
    ACS concepts against the reviewed variable map [432]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    families = {source: family["id"] for family in registry["source_families"] for source in family["audit_source_ids"]}
    expected_controls = {
        control["id"]: control["preserved_controls"]["current_review_decision"]
        for control in registry["measure_controls"]
        if {families[source] for source in control["source_ids"] if source in families} & {"S18", "S19"}
    }
    seed = REPO_ROOT / "dbt/seeds/acs_svi_measures.csv"
    if not seed.exists():
        return False
    with seed.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    with (REPO_ROOT / "dbt/seeds/acs_variable_map.csv").open(newline="") as handle:
        concepts = {row["concept_id"] for row in csv.DictReader(handle)}
    seeded = {row["measure_control"]: row["review_decision"] for row in rows}
    named = {item for row in rows if row["source"] == "acs" for item in row["components"].split()}
    return bool(expected_controls) and seeded == expected_controls and named <= concepts


def income_labor_seed_matches() -> bool:
    """Check the SAIPE, SAHIE and BLS measure seed against the registry: every control of S24, S25 and S29 once per row,
    with its decision [446]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    families = {source: family["id"] for family in registry["source_families"] for source in family["audit_source_ids"]}
    wanted = {"S24", "S25", "S29"}
    expected_rows = sorted(
        (control["id"], control["preserved_controls"]["current_review_decision"])
        for control in registry["measure_controls"]
        if {families[source] for source in control["source_ids"] if source in families} & wanted
    )
    seed = REPO_ROOT / "dbt/seeds/income_labor_measures.csv"
    if not seed.exists():
        return False
    with seed.open(newline="") as handle:
        seeded = sorted((row["measure_control"], row["review_decision"]) for row in csv.DictReader(handle))
    return bool(expected_rows) and seeded == expected_rows


def county_health_seed_matches() -> bool:
    """Check the PLACES, geographic variation and WONDER measure seed against the registry: every control of S20, S28 and
    S30 once, with its decision [460]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    families = {source: family["id"] for family in registry["source_families"] for source in family["audit_source_ids"]}
    expected_rows = sorted(
        (control["id"], control["preserved_controls"]["current_review_decision"])
        for control in registry["measure_controls"]
        if {families[source] for source in control["source_ids"] if source in families} & {"S20", "S28", "S30"}
    )
    seed = REPO_ROOT / "dbt/seeds/county_health_measures.csv"
    if not seed.exists():
        return False
    with seed.open(newline="") as handle:
        seeded = sorted((row["measure_control"], row["review_decision"]) for row in csv.DictReader(handle))
    return bool(expected_rows) and seeded == expected_rows


def validation_seed_matches() -> bool:
    """Check the validation measure seed against the registry: every control of the D1 sources and their children, each named
    ID found in its control's exact field, E038 and E039 unmapped [508] [511]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    controls = {control["id"]: control for control in registry["measure_controls"]}
    names = ("care-hrrp-cms", "care-visits-cms", "care-deaths-cms", "PSI90", "PSI13", "care-hac-cms", "care-vbp-cms")
    parents = {control for name in names for control in sources[name]["linked_measure_ids"]}
    expected = parents | {name for name, control in controls.items() if control.get("parent_id") in parents}
    seed = REPO_ROOT / "dbt/seeds/validation_measures.csv"
    if not seed.exists():
        return False
    with seed.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    # A field is named when it appears in its control's exact field, or its parent's for a child without one (C288 children)
    # [520]; underscores compare as spaces and case is ignored.
    fields = {
        name: (
            controls[name]["preserved_controls"].get("current_exact_field")
            or controls[controls[name]["parent_id"]]["preserved_controls"]["current_exact_field"]
        )
        .lower()
        .replace("_", " ")
        for name in expected
    }
    children_ids = {row["measure_control"]: row["source_measure_id"] for row in rows if controls[row["measure_control"]].get("parent_id") in parents}
    named = all(
        (row["source_measure_id"].lower().replace("_", " ") in fields[row["measure_control"]])
        or (row["measure_control"] in parents and row["source_measure_id"] in children_ids.values())
        for row in rows
        if row["source_measure_id"]
    )
    unmapped = {row["measure_control"] for row in rows if not row["source_measure_id"]} == {"E038", "E039", "C288.payment_adjustment"}
    decisions = all(row["review_decision"] == (controls[row["measure_control"]]["preserved_controls"]["current_review_decision"] or "") for row in rows)
    return {row["measure_control"] for row in rows} == expected and named and unmapped and decisions


def shortage_seed_matches() -> bool:
    """Check the MMD, HPSA and MUA measure seed against the registry and its user additions: every control of S23 and S27
    once, with its decision [474] [482]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    additions = json.loads((REPO_ROOT / "config/acquisition/registry_additions.json").read_text())
    families = {source: family["id"] for family in registry["source_families"] for source in family["audit_source_ids"]}
    expected_rows = sorted(
        (control["id"], control["preserved_controls"]["current_review_decision"])
        for control in [*registry["measure_controls"], *additions["measure_controls"]]
        if {families[source] for source in control["source_ids"] if source in families} & {"S23", "S27"}
    )
    seed = REPO_ROOT / "dbt/seeds/shortage_measures.csv"
    if not seed.exists():
        return False
    with seed.open(newline="") as handle:
        seeded = sorted((row["measure_control"], row["review_decision"]) for row in csv.DictReader(handle))
    return bool(expected_rows) and seeded == expected_rows


def mup_seed_matches() -> bool:
    """Check that the Medicare inpatient measure seed covers exactly the registry's controls of its two sources [363]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    with (REPO_ROOT / "dbt/seeds/mup_measures.csv").open(newline="") as handle:
        seed = sorted({row["measure_control"] for row in csv.DictReader(handle)})
    expected = {*sources["CMS-MUP-PROVIDER"]["linked_measure_ids"], *sources["CMS_MEDICARE_PROVIDER"]["linked_measure_ids"]}
    return seed == sorted(expected)


# Ordered fingerprints of every C1 model, compared between the two real builds [418]. Each row is hashed before the ordered
# aggregate, so an 11.9 million-row model needs about 0.4 GB, not one string of every row (failure mode 544).
GEOGRAPHY_FINGERPRINTS = {
    "int_hud_zip_county_quarters": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY hud_row_key)) FROM int_hud_zip_county_quarters AS t;",
    "int_hud_zip_county_holds": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY hud_row_key)) FROM int_hud_zip_county_holds AS t;",
    "int_county_adjacency_edges": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY edge_key)) FROM int_county_adjacency_edges AS t;",
    "int_rucc_county_codes": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY rucc_key)) FROM int_rucc_county_codes AS t;",
    "int_ruca_codes": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY ruca_row_key)) FROM int_ruca_codes AS t;",
    "int_hsa_zip_cases": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY hsa_row_key)) FROM int_hsa_zip_cases AS t;",
    "int_acs_county_values": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY acs_value_key)) FROM int_acs_county_values AS t;",
    "int_svi_county_values": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY svi_value_key)) FROM int_svi_county_values AS t;",
    "int_saipe_county_estimates": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY saipe_row_key)) FROM int_saipe_county_estimates AS t;",
    "int_sahie_county_rows": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY sahie_row_key)) FROM int_sahie_county_rows AS t;",
    "int_bls_county_series": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY bls_row_key)) FROM int_bls_county_series AS t;",
    "int_places_county_values": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY places_row_key)) FROM int_places_county_values AS t;",
    "int_gv_county_values": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY gv_value_key)) FROM int_gv_county_values AS t;",
    "int_wonder_county_deaths": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY wonder_row_key)) FROM int_wonder_county_deaths AS t;",
    "int_mmd_prevalence": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY mmd_row_key)) FROM int_mmd_prevalence AS t;",
    "int_cc_unplanned_visits_windows": (
        "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY window_key)) FROM int_cc_unplanned_visits_windows AS t;"
    ),
    "int_cc_complications_deaths_windows": (
        "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY window_key)) FROM int_cc_complications_deaths_windows AS t;"
    ),
    "int_cc_hrrp_windows": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY window_key)) FROM int_cc_hrrp_windows AS t;",
    "int_hac_program_years": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY program_key)) FROM int_hac_program_years AS t;",
    "int_vbp_program_years": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY program_key)) FROM int_vbp_program_years AS t;",
    "int_validation_program_values": (
        "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY program_value_key)) FROM int_validation_program_values AS t;"
    ),
    "int_validation_measure_windows": (
        "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY validation_key)) FROM int_validation_measure_windows AS t;"
    ),
    "int_hpsa_components": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY hpsa_row_key)) FROM int_hpsa_components AS t;",
    "int_mua_components": "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY mua_row_key)) FROM int_mua_components AS t;",
}
GEOGRAPHY_RECONCILE_SQL = """SELECT 'hud', (SELECT count(*) FROM stg_hud_zip_county)::VARCHAR,
    ((SELECT count(*) FROM int_hud_zip_county_quarters) + (SELECT count(*) FROM int_hud_zip_county_holds))::VARCHAR
UNION ALL SELECT 'adjacency', ((SELECT count(*) FROM stg_county_adjacency) + (SELECT count(*) FROM stg_county_adjacency_2010_text_lines))::VARCHAR,
    (SELECT count(*) FROM int_county_adjacency_edges)::VARCHAR
UNION ALL SELECT 'rucc_long', (SELECT count(DISTINCT _member_sha256 || ':' || trim(fips)) FROM stg_rucc)::VARCHAR,
    (SELECT count(*) FROM int_rucc_county_codes WHERE vintage = '2023')::VARCHAR
UNION ALL SELECT 'ruca_csv',
    ((SELECT count(*) FROM stg_ruca_tracts_2020) + (SELECT count(*) FROM stg_ruca_zip_2020) + (SELECT count(*) FROM stg_ruca_zip_2010))::VARCHAR,
    (SELECT count(*) FROM int_ruca_codes WHERE sheet_name IS NULL)::VARCHAR
UNION ALL SELECT 'hsa', (SELECT count(*) FROM stg_cms_hsa_csv)::VARCHAR, (SELECT count(*) FROM int_hsa_zip_cases)::VARCHAR;"""
GEOGRAPHY_TWINS_SQL = """WITH csv AS (
    SELECT _member_sha256 AS csv_sha, _release_id AS release_id, count(*) AS n,
        md5(string_agg(trim(zip) || ':' || trim(geoid), ',' ORDER BY trim(zip), trim(geoid))) AS pairs
    FROM stg_hud_zip_county WHERE _release_id LIKE 'HUD_XLSX%' GROUP BY 1, 2
), sheet_rows AS (
    SELECT DISTINCT _member_sha256, _release_id, cells FROM stg_hud_zip_county_sheet_rows WHERE sheet_row > 1
), sheet AS (
    SELECT s._member_sha256 AS sheet_sha, s._release_id AS release_id, count(*) AS n,
        (SELECT count(*) FROM stg_hud_zip_county_sheet_rows r WHERE r._member_sha256 = s._member_sha256 AND r.sheet_row > 1) AS all_rows,
        md5(string_agg(trim(cells[1]) || ':' || trim(cells[2]), ',' ORDER BY trim(cells[1]), trim(cells[2]))) AS pairs
    FROM sheet_rows AS s GROUP BY 1, 2
)
SELECT coalesce(csv.release_id, sheet.release_id), coalesce(sheet.sheet_sha, ''), coalesce(csv.n, 0)::VARCHAR, coalesce(sheet.n, 0)::VARCHAR,
    coalesce(sheet.all_rows, 0)::VARCHAR, coalesce(csv.pairs = sheet.pairs, false)::VARCHAR
FROM csv FULL JOIN sheet ON csv.release_id = sheet.release_id ORDER BY 1;"""


def geography_real(database: str, init: str) -> dict[str, Any]:
    """Reconcile the real C1 models with their staging rows, the HUD workbooks with their CSV twins and the period seed with storage [404] [419]."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, staged, typed in duckdb_csv(database, GEOGRAPHY_RECONCILE_SQL, init):
        counts[name] = {"staged": int(staged), "typed": int(typed)}
        checks[f"geography_{name}_rows_reconcile"] = int(staged) == int(typed) and int(staged) > 0
    holds = duckdb_csv(database, "SELECT hold_reason, count(*)::VARCHAR FROM int_hud_zip_county_holds GROUP BY 1 ORDER BY 1;")
    counts["hud_holds"] = dict(holds)
    checks["geography_hud_holds_are_the_approved_47"] = holds == [["not_county_code", "47"]]
    sheets = duckdb_csv(
        database,
        "SELECT (SELECT count(*) FROM stg_rucc_sheet_rows r JOIN geography_file_periods p ON r._member_sha256 = p.member_sha256 "
        "WHERE p.file_name = '2013-rural-urban-continuum-codes.xls' AND r.sheet_name = 'Rural-urban Continuum Code 2013' AND r.sheet_row > 1)::VARCHAR, "
        "(SELECT count(*) FROM int_rucc_county_codes WHERE vintage = '2013')::VARCHAR, "
        "(SELECT count(*) FROM stg_ruca_sheet_rows r JOIN geography_file_periods p ON r._member_sha256 = p.member_sha256 "
        "WHERE p.file_name = '2010-rural-urban-commuting-area-codes-revised-732019.xlsx' AND r.sheet_name = 'Data' AND r.sheet_row > 2)::VARCHAR, "
        "(SELECT count(*) FROM int_ruca_codes WHERE sheet_name = 'Data')::VARCHAR;",
        init,
    )[0]
    counts["sheets"] = {
        "rucc_2013": {"staged": int(sheets[0]), "typed": int(sheets[1])},
        "ruca_2010_tracts": {"staged": int(sheets[2]), "typed": int(sheets[3])},
    }
    checks["geography_sheet_rows_reconcile"] = sheets[0] == sheets[1] and sheets[2] == sheets[3] and int(sheets[1]) > 0 and int(sheets[3]) > 0
    # The CSV of a workbook holds its distinct rows; only the owner-approved 2014 Q2 workbook repeats rows, by its recorded
    # count, and every other workbook must have none (docs/data_collection.md, 2014 Q2 repeats).
    approved = json.loads((REPO_ROOT / "config/acquisition/hud_xlsx_exact_repeats_2014q2.json").read_text())
    repeats = {quarter["sha256"]: int(quarter["exact_repeat_rows"]) for quarter in approved["quarters"]}
    twins = duckdb_csv(database, GEOGRAPHY_TWINS_SQL, init)
    matching = [row[0] for row in twins if row[5] == "true" and row[2] == row[3] and int(row[4]) - int(row[3]) == repeats.get(row[1], 0) and int(row[2]) > 0]
    counts["hud_workbook_twins"] = {"releases": len(twins), "matching": len(matching), "approved_repeat_rows": sum(int(row[4]) - int(row[3]) for row in twins)}
    checks["geography_hud_workbooks_match_csv_twins"] = len(matching) == len(twins) > 0
    checks["geography_periods_seed_reproduced"] = geography_file_periods.SEED.read_text() == geography_file_periods.as_csv(geography_file_periods.build())
    checks["geography_seed_matches_registry"] = geography_seed_matches()
    return {"checks": checks, "counts": counts}


# County rows of each ACS file times the columns the map names for its vintage, counted from the staging views without the
# model; and SVI county rows per edition [434].
ACS_EXPECTED_SQL = """WITH files AS (
    SELECT p.member_sha256, p.bronze_table, p.vintage FROM geography_file_periods AS p WHERE starts_with(p.bronze_table, 'acs_')
), mapped AS (
    SELECT bronze_table, vintage, count(*) AS columns FROM acs_variable_map WHERE status = 'mapped' GROUP BY 1, 2
), county_rows AS (
    SELECT _member_sha256 AS member_sha256, count(*) AS n FROM (
        SELECT _member_sha256, geo_id FROM stg_acs_dp02
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_dp03
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_dp04
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_dp05
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_s0101
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_s0601
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_s1701
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_s2503
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_s2701
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_b16005
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_b19013
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_b25070
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_b25091
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_b26001
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_c16001
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_summary_b16005
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_summary_b19013
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_summary_b25070
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_summary_b25091
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_summary_b26001
        UNION ALL SELECT _member_sha256, geo_id FROM stg_acs_summary_c16001
    ) WHERE regexp_full_match(trim(geo_id), '0500000US[0-9]{5}') GROUP BY 1
)
SELECT f.bronze_table, f.vintage, (coalesce(c.n, 0) * coalesce(m.columns, 0))::VARCHAR,
    (SELECT count(*) FROM int_acs_county_values AS v WHERE v.member_sha256 = f.member_sha256)::VARCHAR
FROM files AS f LEFT JOIN county_rows AS c ON f.member_sha256 = c.member_sha256
LEFT JOIN mapped AS m ON f.bronze_table = m.bronze_table AND f.vintage = m.vintage ORDER BY 1, 2;"""
SVI_EXPECTED_SQL = """WITH rows AS (
    SELECT s._member_sha256 AS member_sha256, coalesce(nullif(trim(s.fips), ''), trim(s.state_fips) || trim(s.cnty_fips)) AS county
    FROM stg_svi AS s
)
SELECT p.vintage, count(*) FILTER (WHERE regexp_full_match(r.county, '[0-9]{4,5}'))::VARCHAR,
    count(*) FILTER (WHERE NOT coalesce(regexp_full_match(r.county, '[0-9]{4,5}'), false))::VARCHAR,
    (SELECT count(DISTINCT v.member_sha256 || v.source_row_number) FROM int_svi_county_values AS v WHERE v.edition = p.vintage)::VARCHAR
FROM rows AS r JOIN geography_file_periods AS p ON r.member_sha256 = p.member_sha256 GROUP BY 1 ORDER BY 1;"""


# Staged rows counted from the staging views without the models: SAIPE county lines of the all-geography files, SAHIE
# county rows and every BLS row, against the typed rows; and the Alabama-only 2023 SAIPE lines against the full file [437] [448].
INCOME_LABOR_SQL = """WITH saipe AS (
    SELECT l._member_sha256, l.line_text FROM stg_saipe_text_lines AS l
    JOIN geography_file_periods AS p ON l._member_sha256 = p.member_sha256
    WHERE regexp_full_match(p.file_name, 'est[0-9]{2}all\\.(txt|dat)') AND regexp_full_match(substr(l.line_text, 1, 2), '[0-9]{2}')
        AND regexp_full_match(trim(substr(l.line_text, 4, 3)), '[0-9]{1,3}') AND trim(substr(l.line_text, 4, 3))::INTEGER > 0
)
SELECT 'saipe', (SELECT count(*) FROM saipe)::VARCHAR, (SELECT count(*) FROM int_saipe_county_estimates)::VARCHAR
UNION ALL SELECT 'sahie', (SELECT count(*) FROM stg_sahie WHERE trim(geocat) = '50')::VARCHAR, (SELECT count(*) FROM int_sahie_county_rows)::VARCHAR
UNION ALL SELECT 'bls', (SELECT count(*) FROM stg_bls_laus)::VARCHAR, (SELECT count(*) FROM int_bls_county_series)::VARCHAR;"""
SAIPE_TWIN_SQL = """WITH files AS (SELECT member_sha256, file_name FROM geography_file_periods WHERE bronze_table = 'saipe_text_lines'),
state_lines AS (
    SELECT substr(l.line_text, 1, 241) AS line FROM stg_saipe_text_lines AS l JOIN files AS f ON l._member_sha256 = f.member_sha256
    WHERE f.file_name = 'est23-al.txt'
),
full_lines AS (
    SELECT substr(l.line_text, 1, 241) AS line FROM stg_saipe_text_lines AS l JOIN files AS f ON l._member_sha256 = f.member_sha256
    WHERE f.file_name = 'est23all.txt' AND substr(l.line_text, 1, 2) = '01'
)
SELECT (SELECT count(*) FROM state_lines)::VARCHAR, (SELECT count(*) FROM full_lines)::VARCHAR,
    (SELECT count(*) FROM (SELECT line FROM state_lines EXCEPT ALL SELECT line FROM full_lines))::VARCHAR;"""


# Staged rows counted from the staging views without the models: PLACES rows with a county code or a name the other releases
# code one way, the name-crosswalk rows on their own, rows with neither, WONDER rows and the repeated rows held; geographic
# variation county rows at the All level [449] [457] [462] [623] [624].
COUNTY_HEALTH_SQL = """WITH names AS (
    SELECT trim(stateabbr) AS st, trim(locationname) AS nm FROM stg_places WHERE regexp_full_match(trim(locationid), '[0-9]{5}')
    GROUP BY 1, 2 HAVING count(DISTINCT trim(locationid)) = 1
),
crosswalk AS (
    SELECT count(*) AS n FROM stg_places AS s JOIN names ON trim(s.stateabbr) = names.st AND trim(s.locationname) = names.nm
    WHERE nullif(trim(s.locationid), '') IS NULL
)
SELECT 'places', ((SELECT count(*) FROM stg_places WHERE regexp_full_match(trim(locationid), '[0-9]{5}')) + (SELECT n FROM crosswalk))::VARCHAR,
    (SELECT count(*) FROM int_places_county_values)::VARCHAR
UNION ALL SELECT 'places_name_crosswalk', (SELECT n FROM crosswalk)::VARCHAR,
    (SELECT count(*) FROM int_places_county_values WHERE county_source = 'places_name_crosswalk')::VARCHAR
UNION ALL SELECT 'places_without_county',
    ((SELECT count(*) FROM stg_places WHERE NOT coalesce(regexp_full_match(trim(locationid), '[0-9]{5}'), false)) - (SELECT n FROM crosswalk))::VARCHAR, '0'
UNION ALL SELECT 'wonder', (SELECT count(*) FROM stg_wonder_county_mortality)::VARCHAR, (SELECT count(*) FROM int_wonder_county_deaths)::VARCHAR
UNION ALL SELECT 'wonder_held', '3142', (SELECT count(*) FROM int_wonder_county_deaths WHERE hold_reason IS NOT NULL)::VARCHAR
UNION ALL SELECT 'gv_county_rows',
    (SELECT count(*) FROM stg_cms_geographic_variation_csv WHERE trim(bene_geo_lvl) = 'County' AND trim(bene_age_lvl) = 'All')::VARCHAR,
    (SELECT count(DISTINCT member_sha256 || source_row_number) FROM int_gv_county_values)::VARCHAR;"""
PLACES_COVERAGE_SQL = """SELECT edition, data_year::VARCHAR, count(DISTINCT measureid || datavaluetypeid) FILTER (WHERE is_all_states)::VARCHAR,
    count(DISTINCT measureid || datavaluetypeid)::VARCHAR FROM int_places_county_values GROUP BY 1, 2 ORDER BY 1, 2;"""


# Staged rows counted from the staging views without the models: MMD rows, HPSA and MUA rows (kept plus held) and the
# approved repeat counts; unknown-county MMD rows are recorded [470] [475] [484].
SHORTAGE_SQL = """SELECT 'mmd', (SELECT count(*) FROM stg_cms_mmd_csv)::VARCHAR, (SELECT count(*) FROM int_mmd_prevalence)::VARCHAR
UNION ALL SELECT 'mmd_mapped', (SELECT count(*) FROM stg_cms_mmd_csv)::VARCHAR,
    (SELECT count(measure_control) FROM int_mmd_prevalence)::VARCHAR
UNION ALL SELECT 'hpsa', (SELECT count(*) FROM stg_hrsa_hpsa_detail)::VARCHAR, (SELECT count(*) FROM int_hpsa_components)::VARCHAR
UNION ALL SELECT 'hpsa_held', '4', (SELECT count(*) FROM int_hpsa_components WHERE hold_reason IS NOT NULL)::VARCHAR
UNION ALL SELECT 'mua', (SELECT count(*) FROM stg_hrsa_mua_detail)::VARCHAR, (SELECT count(*) FROM int_mua_components)::VARCHAR
UNION ALL SELECT 'mua_held', '358', (SELECT count(*) FROM int_mua_components WHERE hold_reason IS NOT NULL)::VARCHAR
UNION ALL SELECT 'mmd_unknown_county', (SELECT count(*) FROM stg_cms_mmd_csv WHERE trim(geography) = 'County' AND nullif(trim(county), '') IS NULL)::VARCHAR,
    (SELECT count(*) FROM int_mmd_prevalence WHERE is_unknown_county)::VARCHAR;"""
MMD_CONTROLS_SQL = "SELECT count(DISTINCT measure_control)::VARCHAR FROM int_mmd_prevalence;"


# Every distinct usable window key of a D1 table is a window or a held window, never both and never neither; the hospital ID
# follows the models' rule: a 5-digit ID padded, any other ID that is not 6 digits or capital letters null [513] [524].
D1_KEYS_SQL = """WITH keys AS (
    SELECT DISTINCT 'cms_cc_unplanned_hospital_visits_hospital' AS t,
        CASE WHEN regexp_full_match(trim(coalesce(facility_id, provider_id)), '[0-9]{5}') THEN lpad(trim(coalesce(facility_id, provider_id)), 6, '0')
        WHEN regexp_full_match(trim(coalesce(facility_id, provider_id)), '[0-9A-Z]{6}') THEN trim(coalesce(facility_id, provider_id)) END AS e,
        trim(measure_id) AS m,
        try_strptime(coalesce(start_date, measure_start_date), '%m/%d/%Y')::date AS s, try_strptime(coalesce(end_date, measure_end_date), '%m/%d/%Y')::date AS f
    FROM stg_cms_cc_unplanned_hospital_visits_hospital WHERE NOT is_label_held
    UNION ALL
    SELECT DISTINCT 'cms_cc_complications_and_deaths_hospital',
        CASE WHEN regexp_full_match(trim(coalesce(facility_id, provider_id)), '[0-9]{5}') THEN lpad(trim(coalesce(facility_id, provider_id)), 6, '0')
        WHEN regexp_full_match(trim(coalesce(facility_id, provider_id)), '[0-9A-Z]{6}') THEN trim(coalesce(facility_id, provider_id)) END, trim(measure_id),
        try_strptime(coalesce(start_date, measure_start_date), '%m/%d/%Y')::date, try_strptime(coalesce(end_date, measure_end_date), '%m/%d/%Y')::date
    FROM stg_cms_cc_complications_and_deaths_hospital WHERE NOT is_label_held
    UNION ALL
    SELECT DISTINCT 'cms_cc_hospital_readmissions_reduction_program_hospital',
        CASE WHEN regexp_full_match(trim(facility_id), '[0-9]{5}') THEN lpad(trim(facility_id), 6, '0')
        WHEN regexp_full_match(trim(facility_id), '[0-9A-Z]{6}') THEN trim(facility_id) END, trim(measure_name),
        try_strptime(start_date, '%m/%d/%Y')::date, try_strptime(end_date, '%m/%d/%Y')::date
    FROM stg_cms_cc_hospital_readmissions_reduction_program_hospital WHERE NOT is_label_held
),
usable AS (SELECT * FROM keys WHERE nullif(e, '') IS NOT NULL AND nullif(m, '') IS NOT NULL AND s IS NOT NULL AND f IS NOT NULL),
w AS (
    SELECT 'cms_cc_unplanned_hospital_visits_hospital' AS t, entity_id AS e, measure_id AS m, window_start AS s, window_end AS f
    FROM int_cc_unplanned_visits_windows
    UNION ALL SELECT 'cms_cc_complications_and_deaths_hospital', entity_id, measure_id, window_start, window_end FROM int_cc_complications_deaths_windows
    UNION ALL SELECT 'cms_cc_hospital_readmissions_reduction_program_hospital', entity_id, measure_id, window_start, window_end FROM int_cc_hrrp_windows
),
h AS (
    SELECT bronze_table AS t, entity_id AS e, measure_id AS m, window_start AS s, window_end AS f
    FROM int_validation_window_holds WHERE window_start IS NOT NULL
)
SELECT usable.t, count(*)::VARCHAR, (SELECT count(*) FROM w WHERE w.t = usable.t)::VARCHAR, (SELECT count(*) FROM h WHERE h.t = usable.t)::VARCHAR,
    (SELECT count(*) FROM w INNER JOIN h USING (t, e, m, s, f) WHERE w.t = usable.t)::VARCHAR
FROM usable GROUP BY usable.t ORDER BY 1;"""


# Every distinct usable hospital and fiscal year of a D2 table is a program-year row or held, never both and never neither,
# with the models' hospital ID rule [523] [524].
D2_KEYS_SQL = """WITH keys AS (
    SELECT DISTINCT 'cms_cc_hac_reduction_program_hospital' AS t,
        CASE WHEN regexp_full_match(trim(facility_id), '[0-9]{5}') THEN lpad(trim(facility_id), 6, '0')
        WHEN regexp_full_match(trim(facility_id), '[0-9A-Z]{6}') THEN trim(facility_id) END AS c, try_cast(trim(fiscal_year) AS INTEGER) AS y
    FROM stg_cms_cc_hac_reduction_program_hospital WHERE NOT is_label_held
    UNION ALL
    SELECT DISTINCT 'cms_cc_hvbp_tps',
        CASE WHEN regexp_full_match(trim(coalesce(facility_id, provider_number)), '[0-9]{5}') THEN lpad(trim(coalesce(facility_id, provider_number)), 6, '0')
        WHEN regexp_full_match(trim(coalesce(facility_id, provider_number)), '[0-9A-Z]{6}') THEN trim(coalesce(facility_id, provider_number)) END,
        coalesce(try_cast(trim(stg.fiscal_year) AS INTEGER), seed.fiscal_year)
    FROM stg_cms_cc_hvbp_tps AS stg LEFT JOIN vbp_file_fiscal_years AS seed ON stg._member_path = seed.file_name WHERE NOT stg.is_label_held
),
usable AS (SELECT * FROM keys WHERE nullif(c, '') IS NOT NULL AND y IS NOT NULL),
p AS (
    SELECT 'cms_cc_hac_reduction_program_hospital' AS t, ccn AS c, fiscal_year AS y FROM int_hac_program_years
    UNION ALL SELECT 'cms_cc_hvbp_tps', ccn, fiscal_year FROM int_vbp_program_years
),
h AS (SELECT bronze_table AS t, ccn AS c, fiscal_year AS y FROM int_validation_program_holds WHERE fiscal_year IS NOT NULL)
SELECT usable.t, count(*)::VARCHAR, (SELECT count(*) FROM p WHERE p.t = usable.t)::VARCHAR, (SELECT count(*) FROM h WHERE h.t = usable.t)::VARCHAR,
    (SELECT count(*) FROM p INNER JOIN h USING (t, c, y) WHERE p.t = usable.t)::VARCHAR
FROM usable GROUP BY usable.t ORDER BY 1;"""


# AL1 reconciliation [549] to [556]: outcome rows against the spine, and published and held SIRs counted independently
# from the calendar-year HAI windows (release date against the last 2015-baseline review, Aug 13 2026).
OUTCOME_REAL_SQL = """WITH cal AS (
    SELECT measure_id, score, compared_to_national, release_date FROM int_hai_hospital_windows
    WHERE month(window_start) = 1 AND day(window_start) = 1 AND window_end = make_date(year(window_start), 12, 31)
)
SELECT 'rows', (SELECT count(*) FROM int_spine_hai_outcomes)::VARCHAR, (SELECT 6 * count(*) FROM int_hospital_spine)::VARCHAR
UNION ALL SELECT 'keys', (SELECT count(DISTINCT outcome_key) FROM int_spine_hai_outcomes)::VARCHAR, (SELECT 6 * count(*) FROM int_hospital_spine)::VARCHAR
UNION ALL SELECT 'sirs', (SELECT count(*) FROM int_spine_hai_outcomes WHERE has_sir)::VARCHAR,
    (SELECT count(*) FROM cal WHERE right(measure_id, 4) = '_SIR' AND release_date <= DATE '2026-08-13'
        AND regexp_full_match(trim(score), '-?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([eE][-+]?[0-9]+)?'))::VARCHAR
UNION ALL SELECT 'held_parts', (SELECT coalesce(sum(baseline_held_parts), 0) FROM int_spine_hai_outcomes)::VARCHAR,
    (SELECT count(*) FROM cal WHERE release_date > DATE '2026-08-13')::VARCHAR
UNION ALL SELECT 'sir_without_counts', (SELECT count(*) FROM int_spine_hai_outcomes WHERE has_sir AND (observed IS NULL OR predicted IS NULL))::VARCHAR, '0'
UNION ALL SELECT 'c269_values', (SELECT count(*) FROM int_hai_outcome_values WHERE measure_control = 'C269')::VARCHAR,
    (SELECT count(*) FROM cal WHERE measure_id = 'HAI_1_SIR' AND release_date <= DATE '2026-08-13' AND nullif(trim(score), '') IS NOT NULL)::VARCHAR
UNION ALL SELECT 'benchmark_values', (SELECT count(*) FROM int_hai_outcome_values WHERE measure_control = 'C283.benchmark_category')::VARCHAR,
    (SELECT count(*) FROM cal WHERE right(measure_id, 4) = '_SIR' AND release_date <= DATE '2026-08-13'
        AND nullif(trim(compared_to_national), '') IS NOT NULL)::VARCHAR
UNION ALL SELECT 'staging_held_parts', (SELECT coalesce(sum(staging_held_parts), 0) FROM int_spine_hai_outcomes)::VARCHAR,
    (SELECT count(DISTINCT entity_id || measure_id || window_start) FROM int_hai_window_holds WHERE bronze_table = 'cms_hai_hospital'
        AND month(window_start) = 1 AND day(window_start) = 1 AND window_end = make_date(year(window_start), 12, 31))::VARCHAR;"""
OUTCOME_FINGERPRINT_SQL = "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY outcome_key)) FROM int_spine_hai_outcomes AS t;"


def outcome_real(database: str) -> dict[str, Any]:
    """Reconcile the real AL1 outcome with the spine and the calendar-year HAI windows; count SIRs per year and type."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, model, independent in duckdb_csv(database, OUTCOME_REAL_SQL):
        counts[name] = {"model": int(model), "independent": int(independent)}
        checks[f"outcome_{name}_reconcile"] = model == independent
    counts["alignment_status"] = dict(duckdb_csv(database, OUTCOME_STATUS_COUNT_SQL))
    counts["values_per_control"] = dict(duckdb_csv(database, OUTCOME_VALUES_SQL))
    counts["sirs_per_year_type"] = dict(
        duckdb_csv(
            database,
            "SELECT window_year || ' ' || hai_type, count(*) FILTER (WHERE has_sir)::VARCHAR FROM int_spine_hai_outcomes GROUP BY 1 ORDER BY 1;",
        )
    )
    return {"checks": checks, "counts": counts}


# AL2 reconciliation [564] to [572]: rows against the spine and the registry, aligned windows and ratings counted
# independently from the staged windows and releases, and no period on or after the window start.
CARE_REAL_SQL = """WITH spine AS (SELECT spine_key, ccn, make_date(window_year, 1, 1) AS start FROM int_hospital_spine),
dated AS (
    SELECT ccn, release_date, count(DISTINCT coalesce(overall_rating_text, '') || '|' || coalesce(overall_rating_footnote, '')) AS versions
    FROM int_hgi_hospital_releases WHERE ccn IS NOT NULL GROUP BY ALL
),
latest AS (SELECT spine.spine_key, spine.ccn, max(dated.release_date) AS release_date FROM spine JOIN dated
    ON dated.ccn = spine.ccn AND dated.release_date < spine.start GROUP BY ALL)
SELECT 'rows', (SELECT count(*) FROM int_spine_care_compare_measures)::VARCHAR,
    ((SELECT count(*) FROM int_hospital_spine) * (SELECT count(*) FROM registry_measure_sources))::VARCHAR
UNION ALL SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_care_compare_measures)::VARCHAR,
    ((SELECT count(*) FROM int_hospital_spine) * (SELECT count(*) FROM registry_measure_sources))::VARCHAR
UNION ALL SELECT 'aligned_windows', (SELECT count(*) FROM int_spine_care_compare_measures
    WHERE alignment_status = 'aligned' AND measure_control <> 'C284')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, windows.measure_control FROM spine JOIN int_registry_measure_windows AS windows
        ON windows.entity_id = spine.ccn AND windows.window_end < spine.start))::VARCHAR
UNION ALL SELECT 'aligned_ratings', (SELECT count(*) FROM int_spine_care_compare_measures
    WHERE alignment_status = 'aligned' AND measure_control = 'C284')::VARCHAR,
    (SELECT count(*) FROM latest JOIN dated USING (ccn, release_date) WHERE dated.versions = 1)::VARCHAR
UNION ALL SELECT 'held_ratings', (SELECT count(*) FROM int_spine_care_compare_measures
    WHERE alignment_status = 'held_in_staging' AND measure_control = 'C284')::VARCHAR,
    (SELECT count(*) FROM latest JOIN dated USING (ccn, release_date) WHERE dated.versions > 1)::VARCHAR
UNION ALL SELECT 'periods_on_or_after_start', (SELECT count(*) FROM int_spine_care_compare_measures
    WHERE alignment_status = 'aligned' AND period_end >= window_start)::VARCHAR, '0'
UNION ALL SELECT 'c141_unmapped', (SELECT count(*) FROM int_spine_care_compare_measures WHERE measure_control = 'C141'
    AND value_text IS NOT NULL AND value_text <> 'Not Available' AND value_category IS NULL)::VARCHAR, '0';"""
CARE_FINGERPRINT_SQL = "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY alignment_key)) FROM int_spine_care_compare_measures AS t;"


def care_compare_real(database: str) -> dict[str, Any]:
    """Reconcile the real AL2 rows with the spine, the registry windows and the rating releases; count statuses."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, model, independent in duckdb_csv(database, CARE_REAL_SQL):
        counts[name] = {"model": int(model), "independent": int(independent)}
        checks[f"care_compare_{name}_reconcile"] = model == independent
    counts["alignment_status"] = dict(
        duckdb_csv(database, "SELECT alignment_status, count(*)::VARCHAR FROM int_spine_care_compare_measures GROUP BY 1 ORDER BY 1;")
    )
    counts["aligned_per_year"] = dict(
        duckdb_csv(
            database,
            "SELECT window_year::VARCHAR, count(*) FILTER (WHERE alignment_status = 'aligned')::VARCHAR "
            "FROM int_spine_care_compare_measures GROUP BY 1 ORDER BY 1;",
        )
    )
    counts["c141_categories"] = dict(
        duckdb_csv(
            database,
            "SELECT coalesce(value_category, coalesce(value_text, 'none')), count(*)::VARCHAR FROM int_spine_care_compare_measures "
            "WHERE measure_control = 'C141' AND alignment_status = 'aligned' GROUP BY 1 ORDER BY 1;",
        )
    )
    return {"checks": checks, "counts": counts}


# AL3a reconciliation [576] to [583]: rows against the spine and the four seeds, and per source the hospital-windows and
# control-fields with a period before the start, counted independently from the staged tables.
HOSPITAL_REAL_SQL = """WITH spine AS (SELECT spine_key, ccn, window_year, make_date(window_year, 1, 1) AS start FROM int_hospital_spine),
controls AS (
    SELECT count(*) AS n FROM (
        SELECT measure_control FROM cost_report_measures UNION ALL SELECT measure_control FROM impact_measures
        UNION ALL SELECT DISTINCT measure_control || coalesce(field, 'none') FROM mup_measures UNION ALL SELECT measure_control FROM occmix_measures
    )
),
model AS (SELECT measure_source, count(*) FILTER (WHERE period_end IS NOT NULL) AS n FROM int_spine_hospital_measures GROUP BY 1)
SELECT 'rows', (SELECT count(*) FROM int_spine_hospital_measures)::VARCHAR, ((SELECT count(*) FROM spine) * (SELECT n FROM controls))::VARCHAR
UNION ALL SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_hospital_measures)::VARCHAR,
    ((SELECT count(*) FROM spine) * (SELECT n FROM controls))::VARCHAR
UNION ALL SELECT 'cost_report_periods', (SELECT n FROM model WHERE measure_source = 'cost_report')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, m.measure_control FROM spine JOIN int_cost_report_measures AS m ON m.ccn = spine.ccn
        JOIN int_cost_reports AS r ON r.rpt_rec_num = m.rpt_rec_num WHERE r.period_end < spine.start))::VARCHAR
UNION ALL SELECT 'ipps_impact_periods', (SELECT n FROM model WHERE measure_source = 'ipps_impact')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, m.measure_control, m.field FROM spine JOIN int_impact_measures AS m
        ON m.ccn = spine.ccn WHERE m.rule_fiscal_year <= spine.window_year - 1))::VARCHAR
UNION ALL SELECT 'medicare_inpatient_periods', (SELECT n FROM model WHERE measure_source = 'medicare_inpatient')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, m.measure_control, m.field FROM spine JOIN int_mup_measures AS m
        ON m.ccn = spine.ccn WHERE m.data_year < spine.window_year))::VARCHAR
UNION ALL SELECT 'occupational_mix_periods', (SELECT n FROM model WHERE measure_source = 'occupational_mix')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, m.measure_control FROM spine JOIN int_occmix_measures AS m ON m.ccn = spine.ccn
        WHERE m.survey_end_date < spine.start AND m.hold_reason IS NULL AND m.value_number IS NOT NULL))::VARCHAR
UNION ALL SELECT 'periods_on_or_after_start', (SELECT count(*) FROM int_spine_hospital_measures
    WHERE alignment_status = 'aligned' AND period_end >= window_start)::VARCHAR, '0';"""
HOSPITAL_FINGERPRINT_SQL = "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY alignment_key)) FROM int_spine_hospital_measures AS t;"


def hospital_measures_real(database: str) -> dict[str, Any]:
    """Reconcile the real AL3a rows with the spine and the staged sources; count statuses per source."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, model, independent in duckdb_csv(database, HOSPITAL_REAL_SQL):
        counts[name] = {"model": int(model), "independent": int(independent)}
        checks[f"hospital_measures_{name}_reconcile"] = model == independent
    status_sql = "SELECT measure_source || ' ' || alignment_status, count(*)::VARCHAR FROM int_spine_hospital_measures GROUP BY 1 ORDER BY 1;"
    counts["alignment_status"] = dict(duckdb_csv(database, status_sql))
    return {"checks": checks, "counts": counts}


# AL3b reconciliation [587] to [595]: rows against the spine and the two seeds; HHS, ONC and change-of-ownership periods
# counted independently from the staged tables; no aligned period on or after the start.
OPERATIONS_REAL_SQL = """WITH spine AS (SELECT spine_key, ccn, window_year, make_date(window_year, 1, 1) AS start FROM int_hospital_spine),
controls AS (SELECT (SELECT count(*) FROM hhs_onc_measures) + (SELECT count(*) FROM ownership_measures) AS n),
model AS (SELECT measure_source, measure_control, count(*) FILTER (WHERE period_end IS NOT NULL) AS n FROM int_spine_operations_measures GROUP BY ALL)
SELECT 'rows', (SELECT count(*) FROM int_spine_operations_measures)::VARCHAR, ((SELECT count(*) FROM spine) * (SELECT n FROM controls))::VARCHAR
UNION ALL SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_operations_measures)::VARCHAR,
    ((SELECT count(*) FROM spine) * (SELECT n FROM controls))::VARCHAR
UNION ALL SELECT 'hhs_years', (SELECT n FROM model WHERE measure_control = 'C260')::VARCHAR,
    (SELECT count(DISTINCT spine.spine_key) FROM spine JOIN int_hhs_capacity_weeks AS w ON w.ccn = spine.ccn
        AND year(w.collection_week) = spine.window_year - 1)::VARCHAR
UNION ALL SELECT 'onc_periods', (SELECT n FROM model WHERE measure_control = 'C073')::VARCHAR,
    (SELECT count(DISTINCT spine.spine_key) FROM spine JOIN int_onc_chpl_linkage_rows AS o ON o.ccn = spine.ccn AND o.end_date < spine.start)::VARCHAR
UNION ALL SELECT 'chow_hospital_windows', (SELECT n FROM model WHERE measure_control = 'C070')::VARCHAR,
    (SELECT count(DISTINCT spine.spine_key) FROM spine JOIN int_change_of_ownership_rows AS c
        ON (c.ccn_buyer = spine.ccn OR c.ccn_seller = spine.ccn) AND c.effective_date < spine.start)::VARCHAR
UNION ALL SELECT 'periods_on_or_after_start', (SELECT count(*) FROM int_spine_operations_measures
    WHERE alignment_status = 'aligned' AND period_end >= window_start)::VARCHAR, '0';"""
OPERATIONS_FINGERPRINT_SQL = "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY alignment_key)) FROM int_spine_operations_measures AS t;"


def operations_measures_real(database: str) -> dict[str, Any]:
    """Reconcile the real AL3b rows with the spine and the staged sources; count statuses per source."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, model, independent in duckdb_csv(database, OPERATIONS_REAL_SQL):
        counts[name] = {"model": int(model), "independent": int(independent)}
        checks[f"operations_measures_{name}_reconcile"] = model == independent
    status_sql = "SELECT measure_source || ' ' || alignment_status, count(*)::VARCHAR FROM int_spine_operations_measures GROUP BY 1 ORDER BY 1;"
    counts["alignment_status"] = dict(duckdb_csv(database, status_sql))
    return {"checks": checks, "counts": counts}


# AL5 reconciliation [597] to [601]: rows against the spine and the seed; aligned rows counted independently as the
# control-measures with a published period overlapping the HAI window; no aligned period outside the window.
VALIDATION_ALIGNED_REAL_SQL = """WITH spine AS (SELECT spine_key, ccn, make_date(window_year, 1, 1) AS start, make_date(window_year, 12, 31) AS finish
    FROM int_hospital_spine),
programs AS (
    SELECT fiscal_year, min(try_strptime(hai_measures_start_date, '%m/%d/%Y'))::DATE AS s, max(try_strptime(hai_measures_end_date, '%m/%d/%Y'))::DATE AS e
    FROM int_hac_program_years GROUP BY 1
)
SELECT 'rows', (SELECT count(*) FROM int_spine_validation_measures)::VARCHAR,
    ((SELECT count(*) FROM spine) * (SELECT count(*) FROM validation_measures))::VARCHAR
UNION ALL SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_validation_measures)::VARCHAR,
    ((SELECT count(*) FROM spine) * (SELECT count(*) FROM validation_measures))::VARCHAR
UNION ALL SELECT 'aligned_windows', (SELECT count(*) FROM int_spine_validation_measures WHERE alignment_status = 'aligned' AND fiscal_year IS NULL)::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, w.measure_control, w.measure_id FROM spine JOIN int_validation_measure_windows AS w
        ON w.entity_id = spine.ccn AND w.window_start <= spine.finish AND w.window_end >= spine.start))::VARCHAR
UNION ALL SELECT 'aligned_program_years', (SELECT count(*) FROM int_spine_validation_measures
    WHERE alignment_status = 'aligned' AND fiscal_year IS NOT NULL)::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, v.measure_control, v.field FROM spine JOIN int_validation_program_values AS v ON v.ccn = spine.ccn
        JOIN programs ON programs.fiscal_year = v.fiscal_year AND programs.s <= spine.finish AND programs.e >= spine.start))::VARCHAR
UNION ALL SELECT 'periods_outside_window', (SELECT count(*) FROM int_spine_validation_measures
    WHERE alignment_status = 'aligned' AND (period_end < window_start OR period_start > make_date(window_year, 12, 31)))::VARCHAR, '0';"""
VALIDATION_ALIGNED_FINGERPRINT_SQL = (
    "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY alignment_key)) FROM int_spine_validation_measures AS t;"
)


def validation_aligned_real(database: str) -> dict[str, Any]:
    """Reconcile the real AL5 rows with the spine, the seed and the staged validation tables; count statuses."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, model, independent in duckdb_csv(database, VALIDATION_ALIGNED_REAL_SQL):
        counts[name] = {"model": int(model), "independent": int(independent)}
        checks[f"validation_aligned_{name}_reconcile"] = model == independent
    counts["alignment_status"] = dict(duckdb_csv(database, VALIDATION_ALIGNED_COUNT_SQL))
    return {"checks": checks, "counts": counts}


def validation_kept_apart() -> bool:
    """[599] No model reads the validation table: it is compared with the HAI windows, never joined to predictors."""
    return not any("ref('int_spine_validation_measures')" in path.read_text() for path in (REPO_ROOT / "dbt/models").rglob("*.sql"))


# AL4a reconciliation [604] to [611]: rows against the spine and the seeds; per source the hospital-windows and
# control-fields with a data year before the start through the POS county, counted from the staged tables.
COUNTY_REAL_SQL = """WITH spine AS (SELECT spine_key, county_fips, planning_region_fips, window_year FROM int_hospital_spine),
model AS (SELECT measure_source, count(*) FILTER (WHERE period_end IS NOT NULL) AS n FROM int_spine_county_measures GROUP BY 1)
SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_county_measures)::VARCHAR, (SELECT count(*) FROM int_spine_county_measures)::VARCHAR
UNION ALL SELECT 'rows_per_spine_row', (SELECT count(*) FROM int_spine_county_measures)::VARCHAR,
    ((SELECT count(*) FROM spine) * (SELECT count(DISTINCT measure_control || ':' || field) FROM int_spine_county_measures))::VARCHAR
UNION ALL SELECT 'mmd_periods', (SELECT n FROM model WHERE measure_source = 'mmd')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, m.measure_control FROM spine JOIN int_mmd_prevalence AS m
        ON m.county_fips IN (spine.county_fips, spine.planning_region_fips) AND m.geography_level = 'county' AND m.data_year < spine.window_year))::VARCHAR
UNION ALL SELECT 'rucc_periods', (SELECT n FROM model WHERE measure_source = 'rucc')::VARCHAR,
    (SELECT count(DISTINCT spine.spine_key) FROM spine JOIN int_rucc_county_codes AS r
        ON r.county_fips IN (spine.county_fips, spine.planning_region_fips) AND r.vintage::INTEGER < spine.window_year)::VARCHAR
UNION ALL SELECT 'places_periods', (SELECT n FROM model WHERE measure_source = 'places')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, p.measureid, p.datavaluetypeid FROM spine JOIN int_places_county_values AS p
        ON p.county_fips IN (spine.county_fips, spine.planning_region_fips) AND p.is_all_states AND p.data_year < spine.window_year
        JOIN county_health_measures AS c ON c.source_model = 'int_places_county_values' AND c.components = p.measureid))::VARCHAR
UNION ALL SELECT 'periods_on_or_after_start', (SELECT count(*) FROM int_spine_county_measures
    WHERE alignment_status = 'aligned' AND period_end >= window_start)::VARCHAR, '0';"""
COUNTY_FINGERPRINT_SQL = "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY alignment_key)) FROM int_spine_county_measures AS t;"
# AL4b: each value source's eligible periods recounted from the staged tables, and the shape of the table [611] [634] [636].
AL4B_REAL_SQL = """WITH spine AS (SELECT spine_key, county_fips, planning_region_fips, window_year, window_start FROM int_hospital_spine),
model AS (SELECT measure_source, count(*) FILTER (WHERE period_end IS NOT NULL) AS n FROM int_spine_county_context
    WHERE measure_source IN ('acs', 'svi') GROUP BY 1),
acs_fields AS (SELECT DISTINCT measure_control, unnest(string_split(components, ' ')) AS field FROM acs_svi_measures WHERE source = 'acs'),
svi_fields AS (SELECT DISTINCT measure_control, unnest(string_split(components, ' ')) AS field FROM acs_svi_measures WHERE source = 'svi')
SELECT 'keys', (SELECT count(DISTINCT alignment_key) FROM int_spine_county_context)::VARCHAR, (SELECT count(*) FROM int_spine_county_context)::VARCHAR
UNION ALL SELECT 'rows_per_spine_row', (SELECT count(*) FROM int_spine_county_context)::VARCHAR,
    ((SELECT count(*) FROM spine) * (SELECT count(DISTINCT measure_source || ':' || measure_control || ':' || field) FROM int_spine_county_context))::VARCHAR
UNION ALL SELECT 'acs_periods', (SELECT n FROM model WHERE measure_source = 'acs')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, f.measure_control, f.field FROM spine JOIN int_acs_county_values AS a
        ON a.county_fips IN (spine.county_fips, spine.planning_region_fips) AND a.vintage::INTEGER < spine.window_year
        JOIN acs_fields AS f ON f.field = a.concept_id))::VARCHAR
UNION ALL SELECT 'svi_periods', (SELECT n FROM model WHERE measure_source = 'svi')::VARCHAR,
    (SELECT count(*) FROM (SELECT DISTINCT spine.spine_key, f.measure_control, f.field FROM spine JOIN int_svi_county_values AS s
        ON s.county_fips IN (spine.county_fips, spine.planning_region_fips) AND s.edition::INTEGER < spine.window_year
        JOIN svi_fields AS f ON f.field = s.field))::VARCHAR
UNION ALL SELECT 'hpsa_windows_in_force', (SELECT count(*) FROM int_spine_county_context
        WHERE measure_source = 'hpsa' AND field = 'designations_in_force' AND value_number > 0)::VARCHAR,
    (SELECT count(DISTINCT spine.spine_key) FROM spine JOIN int_hpsa_components AS h
        ON h.county_fips IN (spine.county_fips, spine.planning_region_fips) AND h.designation_date < spine.window_start
        AND (h.withdrawn_date IS NULL OR h.withdrawn_date > spine.window_start) AND NOT (h.hpsa_status = 'Withdrawn' AND h.withdrawn_date IS NULL)
        AND h.designation_date <> DATE '1970-01-01' AND h.hold_reason IS NULL
        WHERE spine.spine_key NOT IN (SELECT spine_key FROM int_spine_county_context
            WHERE measure_source = 'hpsa' AND alignment_status = 'held_in_staging'))::VARCHAR
UNION ALL SELECT 'linkage_rows_per_spine_row', (SELECT count(*) FROM int_spine_linkage)::VARCHAR, ((SELECT count(*) FROM spine) * 3)::VARCHAR
UNION ALL SELECT 'periods_on_or_after_start', ((SELECT count(*) FROM int_spine_county_context
    WHERE alignment_status = 'aligned' AND period_end >= window_start)
    + (SELECT count(*) FROM int_spine_linkage WHERE alignment_status = 'aligned' AND period_end >= window_start))::VARCHAR, '0';"""
AL4B_LINKAGE_FINGERPRINT_SQL = "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY alignment_key)) FROM int_spine_linkage AS t;"
AL4B_FINGERPRINT_SQL = "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(t)), '' ORDER BY alignment_key)) FROM int_spine_county_context AS t;"


def al4b_context_real(database: str) -> dict[str, Any]:
    """Reconcile the real AL4b rows with the spine and the staged sources; count statuses per source."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, model, independent in duckdb_csv(database, AL4B_REAL_SQL):
        counts[name] = {"model": int(model), "independent": int(independent)}
        checks[f"al4b_context_{name}_reconcile"] = model == independent
    counts["alignment_status"] = dict(duckdb_csv(database, AL4B_COUNT_SQL))
    return {"checks": checks, "counts": counts}


def county_measures_real(database: str) -> dict[str, Any]:
    """Reconcile the real AL4a rows with the spine and the staged county sources; count statuses per source."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, model, independent in duckdb_csv(database, COUNTY_REAL_SQL):
        counts[name] = {"model": int(model), "independent": int(independent)}
        checks[f"county_measures_{name}_reconcile"] = model == independent
    counts["alignment_status"] = dict(duckdb_csv(database, COUNTY_COUNT_SQL))
    return {"checks": checks, "counts": counts}


def validation_real(database: str, init: str) -> dict[str, Any]:
    """Reconcile the real D1 windows with their staging views' window keys; count rows per control [511] [513]."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for table, keys, windows, held, both in duckdb_csv(database, D1_KEYS_SQL, init):
        counts[table] = {"window_keys": int(keys), "windows": int(windows), "held_windows": int(held), "both": int(both)}
        checks[f"validation_{table}_keys_reconcile"] = int(keys) == int(windows) + int(held) and int(both) == 0 and int(windows) > 0
    for table, keys, rows, held, both in duckdb_csv(database, D2_KEYS_SQL, init):
        counts[table] = {"hospital_years": int(keys), "rows": int(rows), "held": int(held), "both": int(both)}
        checks[f"validation_{table}_years_reconcile"] = int(keys) == int(rows) + int(held) and int(both) == 0 and int(rows) > 0
    counts["program_rows_per_control"] = dict(
        duckdb_csv(database, "SELECT measure_control, count(*)::VARCHAR FROM int_validation_program_values GROUP BY 1 ORDER BY 1;", init)
    )
    counts["rows_per_control"] = dict(
        duckdb_csv(database, "SELECT measure_control, count(*)::VARCHAR FROM int_validation_measure_windows GROUP BY 1 ORDER BY 1;", init)
    )
    counts["holds"] = dict(
        duckdb_csv(database, "SELECT bronze_table || ' ' || hold_reason, sum(row_count)::VARCHAR FROM int_validation_window_holds GROUP BY 1 ORDER BY 1;", init)
    )
    checks["validation_mapped_controls_have_rows"] = all(
        control in counts["rows_per_control"] for control in ("C289", "C290", "C291", "C289.01", "C290.01", "C291.01")
    )
    checks["validation_seed_matches_registry"] = validation_seed_matches()
    return {"checks": checks, "counts": counts}


def shortage_real(database: str, init: str) -> dict[str, Any]:
    """Reconcile the real C5 models with their staging views; check the condition map reproduces from the pinned plans [468] [484]."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, staged, typed in duckdb_csv(database, SHORTAGE_SQL, init):
        counts[name] = {"staged": int(staged), "typed": int(typed)}
        checks[f"shortage_{name}_reconcile"] = staged == typed and int(staged) > 0
    counts["mmd_controls"] = int(duckdb_csv(database, MMD_CONTROLS_SQL, init)[0][0])
    checks["shortage_mmd_all_controls_staged"] = counts["mmd_controls"] == 81
    checks["shortage_seed_matches_registry"] = shortage_seed_matches()
    checks["mmd_conditions_seed_reproduced"] = mmd_conditions.SEED.read_text() == mmd_conditions.as_csv(mmd_conditions.build())
    return {"checks": checks, "counts": counts}


def county_health_real(database: str, init: str) -> dict[str, Any]:
    """Reconcile the real C4 models with their staging views; record PLACES all-state coverage per release and year [450] [462]."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, staged, typed in duckdb_csv(database, COUNTY_HEALTH_SQL, init):
        counts[name] = {"staged": int(staged), "typed": int(typed)}
        if name != "places_without_county":
            checks[f"county_health_{name}_reconcile"] = staged == typed and int(staged) > 0
    counts["places_all_state_measures"] = {
        f"{edition}:{year}": f"{covered} of {total}" for edition, year, covered, total in duckdb_csv(database, PLACES_COVERAGE_SQL, init)
    }
    checks["county_health_seed_matches_registry"] = county_health_seed_matches()
    return {"checks": checks, "counts": counts}


def income_labor_real(database: str, init: str) -> dict[str, Any]:
    """Reconcile the real C3 models with their staging views and the Alabama-only SAIPE file with its full-file twin [437] [448]."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    for name, staged, typed in duckdb_csv(database, INCOME_LABOR_SQL, init):
        counts[name] = {"staged": int(staged), "typed": int(typed)}
        checks[f"income_labor_{name}_rows_reconcile"] = staged == typed and int(staged) > 0
    state_rows, full_rows, unmatched = duckdb_csv(database, SAIPE_TWIN_SQL, init)[0]
    counts["saipe_alabama_twin"] = {"state_file_rows": int(state_rows), "full_file_rows": int(full_rows), "unmatched": int(unmatched)}
    checks["income_labor_saipe_state_file_is_twin"] = state_rows == full_rows and unmatched == "0" and int(state_rows) > 0
    checks["income_labor_seed_matches_registry"] = income_labor_seed_matches()
    return {"checks": checks, "counts": counts}


def county_context_real(database: str, init: str) -> dict[str, Any]:
    """Reconcile the real C2 models with their staging views, independently of the model SQL [434]."""
    checks: dict[str, bool] = {}
    counts: dict[str, Any] = {}
    acs = duckdb_csv(database, ACS_EXPECTED_SQL, init)
    counts["acs_files"] = {f"{table}:{vintage}": {"expected": int(expected), "typed": int(typed)} for table, vintage, expected, typed in acs}
    counts["acs_values"] = sum(int(row[3]) for row in acs)
    checks["county_context_acs_values_reconcile"] = bool(acs) and all(row[2] == row[3] for row in acs) and counts["acs_values"] > 0
    svi = duckdb_csv(database, SVI_EXPECTED_SQL, init)
    counts["svi_editions"] = {edition: {"county_rows": int(rows), "other_rows": int(other), "typed_rows": int(typed)} for edition, rows, other, typed in svi}
    checks["county_context_svi_rows_reconcile"] = len(svi) == 7 and all(row[1] == row[3] for row in svi)
    checks["county_context_svi_label_row_only_in_2000"] = [(row[0], row[2]) for row in svi if row[2] != "0"] == [("2000", "1")]
    checks["county_context_seed_matches_registry"] = acs_svi_seed_matches()
    return {"checks": checks, "counts": counts}


def fingerprint(database: str, query: str) -> list[list[str]]:
    """Return a model's fingerprint, or a marker when the build skipped the model, so the report shows the failed build."""
    try:
        return duckdb_csv(database, query)
    except RuntimeError as error:
        return [["missing", str(error)[-200:]]]


def real_stage(once: bool = False) -> dict[str, Any]:
    """Build the models from the catalog twice (once in an iteration run) and reconcile them with bronze [695] [696]."""
    outcome: dict[str, Any] = {"checks": {}, "counts": {}, "rebuild_checks_skipped": [], "docker_release": []}

    def rebuilt(name: str, *keys: str) -> None:
        """Compare the two builds' fingerprints; an iteration run names the check as skipped instead [695]."""
        if once:
            outcome["rebuild_checks_skipped"].append(name)
        else:
            outcome["checks"][name] = all(outcome[f"real_{key}"] == outcome[f"real_again_{key}"] for key in keys)

    stored = ipps_file_labels.collect()
    rebuilt_labels = ipps_file_labels.as_csv(ipps_file_labels.labels(stored, ipps_file_labels.load_overrides()), ipps_file_labels.LABEL_COLUMNS)
    rebuilt_twins = ipps_file_labels.as_csv(ipps_file_labels.twins(stored, ipps_file_labels.load_renamed()), ipps_file_labels.TWIN_COLUMNS)
    outcome["checks"]["generator_reproduces_seeds"] = (
        ipps_file_labels.LABELS_SEED.read_text() == rebuilt_labels and ipps_file_labels.TWINS_SEED.read_text() == rebuilt_twins
    )
    outcome["checks"]["pos_periods_seed_reproduced"] = pos_file_periods.SEED.read_text() == pos_file_periods.as_csv(pos_file_periods.build())
    database = f"{CONTAINER_OUT}/staging.duckdb"
    (OUT / "real_attach.sql").write_text(REAL_ATTACH)
    init = f"{CONTAINER_OUT}/real_attach.sql"
    builds = []
    if once:
        # No check reads a second build in an iteration run, and none is left over from an earlier run [696].
        shutil.rmtree(CASES / "real_again", ignore_errors=True)
    for run in ("real",) if once else ("real", "real_again"):
        (CASES / run).mkdir(parents=True, exist_ok=True)
        code, statuses = dbt_build(run, "lakehouse")
        failed = sorted(name for name, status in statuses.items() if status not in ("pass", "success"))
        outcome["checks"][f"{run}_build_passes"] = code == 0 and bool(statuses) and not failed
        outcome[f"{run}_not_passing"] = failed
        builds.append([row[:6] for row in duckdb_csv(database, FILES_SQL)])
        outcome[f"{run}_occmix_fingerprint"] = duckdb_csv(
            database,
            "SELECT count(*)::VARCHAR, md5(string_agg(md5(to_json(s)), '' ORDER BY survey_row_key)) FROM int_occmix_survey_rows AS s;",
        )
        outcome[f"{run}_geography_fingerprints"] = {model: fingerprint(database, query) for model, query in GEOGRAPHY_FINGERPRINTS.items()}
        outcome[f"{run}_outcome_fingerprint"] = fingerprint(database, OUTCOME_FINGERPRINT_SQL)
        outcome[f"{run}_care_compare_fingerprint"] = fingerprint(database, CARE_FINGERPRINT_SQL)
        outcome[f"{run}_hospital_measures_fingerprint"] = fingerprint(database, HOSPITAL_FINGERPRINT_SQL)
        outcome[f"{run}_operations_measures_fingerprint"] = fingerprint(database, OPERATIONS_FINGERPRINT_SQL)
        outcome[f"{run}_validation_aligned_fingerprint"] = fingerprint(database, VALIDATION_ALIGNED_FINGERPRINT_SQL)
        outcome[f"{run}_county_measures_fingerprint"] = fingerprint(database, COUNTY_FINGERPRINT_SQL)
        outcome[f"{run}_al4b_context_fingerprint"] = fingerprint(database, AL4B_FINGERPRINT_SQL)
        outcome[f"{run}_al4b_linkage_fingerprint"] = fingerprint(database, AL4B_LINKAGE_FINGERPRINT_SQL)
        if run == "real" and not once:
            # Free the memory the first build left in Docker's VM before the second build; the catalog starts again [545] [547].
            outcome["docker_release"].append(catalog.release())
            catalog.up()
    if once:
        outcome["rebuild_checks_skipped"].append("real_rebuild_identical")
    else:
        outcome["checks"]["real_rebuild_identical"] = builds[0] == builds[1]
    outcome["memory_budgets"] = BUDGETS
    rebuilt("occmix_real_rebuild_identical", "occmix_fingerprint")
    rebuilt("geography_real_rebuild_identical", "geography_fingerprints")
    geography = geography_real(database, init)
    outcome["checks"].update(geography["checks"])
    outcome["geography_counts"] = geography["counts"]
    county_context = county_context_real(database, init)
    outcome["checks"].update(county_context["checks"])
    outcome["county_context_counts"] = county_context["counts"]
    income_labor = income_labor_real(database, init)
    outcome["checks"].update(income_labor["checks"])
    outcome["income_labor_counts"] = income_labor["counts"]
    county_health = county_health_real(database, init)
    outcome["checks"].update(county_health["checks"])
    outcome["county_health_counts"] = county_health["counts"]
    shortage = shortage_real(database, init)
    outcome["checks"].update(shortage["checks"])
    outcome["shortage_counts"] = shortage["counts"]
    rebuilt("outcome_real_rebuild_identical", "outcome_fingerprint")
    hai_outcome = outcome_real(database)
    outcome["checks"].update(hai_outcome["checks"])
    outcome["outcome_counts"] = hai_outcome["counts"]
    rebuilt("care_compare_real_rebuild_identical", "care_compare_fingerprint")
    care_compare = care_compare_real(database)
    outcome["checks"].update(care_compare["checks"])
    outcome["al2_care_compare_counts"] = care_compare["counts"]
    rebuilt("hospital_measures_real_rebuild_identical", "hospital_measures_fingerprint")
    hospital = hospital_measures_real(database)
    outcome["checks"].update(hospital["checks"])
    outcome["al3a_hospital_measure_counts"] = hospital["counts"]
    rebuilt("operations_measures_real_rebuild_identical", "operations_measures_fingerprint")
    operations = operations_measures_real(database)
    outcome["checks"].update(operations["checks"])
    outcome["al3b_operations_measure_counts"] = operations["counts"]
    rebuilt("validation_aligned_real_rebuild_identical", "validation_aligned_fingerprint")
    validation_aligned = validation_aligned_real(database)
    outcome["checks"].update(validation_aligned["checks"])
    outcome["al5_validation_counts"] = validation_aligned["counts"]
    rebuilt("county_measures_real_rebuild_identical", "county_measures_fingerprint")
    county = county_measures_real(database)
    outcome["checks"].update(county["checks"])
    outcome["al4a_county_measure_counts"] = county["counts"]
    rebuilt("al4b_context_real_rebuild_identical", "al4b_context_fingerprint", "al4b_linkage_fingerprint")
    al4b = al4b_context_real(database)
    outcome["checks"].update(al4b["checks"])
    outcome["al4b_county_context_counts"] = al4b["counts"]
    validation = validation_real(database, init)
    outcome["checks"].update(validation["checks"])
    outcome["validation_counts"] = validation["counts"]
    status_sql = "SELECT text_layout || ' ' || twin_status, count(*)::VARCHAR FROM stg_bronze__twin_comparison GROUP BY 1 ORDER BY 1;"
    outcome["twin_statuses"] = dict(duckdb_csv(database, status_sql))
    held_sql = (
        "SELECT 'held ' || is_label_held || ', twin excluded ' || is_twin_excluded, count(*)::VARCHAR "
        "FROM stg_bronze__file_selection WHERE NOT is_selected GROUP BY 1 ORDER BY 1;"
    )
    outcome["not_selected"] = dict(duckdb_csv(database, held_sql))
    sheet_sql = "SELECT sheet_status, count(*)::VARCHAR FROM stg_bronze__sheet_selection GROUP BY 1 ORDER BY 1;"
    outcome["sheet_statuses"] = dict(duckdb_csv(database, sheet_sql))
    # BRZ-016: the FY 2020 correction-notice sheet and the FY 2019 proposed-rule sheets are selected [267] [268] [274].
    brz016_sql = (
        "SELECT left(member_sha256, 10) || ' ' || sheet_name, sheet_status FROM stg_bronze__sheet_selection "
        "WHERE (left(member_sha256, 10) = 'ac5ffd206e' AND sheet_name = 'CN 2020') "
        "OR (left(member_sha256, 10) = '6907b7e751' AND sheet_name IN ('FY19 NPRM', 'Variable Descriptions')) ORDER BY 1;"
    )
    outcome["checks"]["brz016_sheets_kept"] = duckdb_csv(database, brz016_sql) == [
        ["6907b7e751 FY19 NPRM", "kept"],
        ["6907b7e751 Variable Descriptions", "kept"],
        ["ac5ffd206e CN 2020", "kept"],
    ]
    # [282] Hospital rows per POS file equal bronze's category 01 rows; every file has a period [280].
    model_pos = dict(duckdb_csv(database, "SELECT member_sha256, count(*)::VARCHAR FROM int_pos_hospital_snapshots GROUP BY 1 ORDER BY 1;"))
    bronze_pos_sql = (
        "SELECT _member_sha256, count(*)::VARCHAR FROM lakehouse.bronze.cms_provider_of_services "
        "WHERE lpad(trim(prvdr_ctgry_cd), 2, '0') = '01' GROUP BY 1 ORDER BY 1;"
    )
    bronze_pos = dict(duckdb_csv(database, bronze_pos_sql, init))
    outcome["checks"]["pos_hospital_rows_reconcile"] = bool(model_pos) and model_pos == bronze_pos
    pos_counts_sql = (
        "SELECT count(DISTINCT period_end)::VARCHAR, count(*)::VARCHAR, count(*) FILTER (WHERE county_fips IS NULL)::VARCHAR, "
        "count(*) FILTER (WHERE NOT is_state_or_dc)::VARCHAR, count(*) FILTER (WHERE is_veterans_affairs)::VARCHAR, "
        "count(*) FILTER (WHERE is_critical_access_by_ccn)::VARCHAR, count(*) FILTER (WHERE ccn IS NULL)::VARCHAR FROM int_pos_hospital_snapshots;"
    )
    names = ("periods", "rows", "no_county", "outside_states_dc", "veterans_affairs", "critical_access_by_ccn", "no_ccn")
    outcome["pos_counts"] = dict(zip(names, (int(value) for value in duckdb_csv(database, pos_counts_sql)[0]), strict=True))
    # [293] to [304] CMI: rows per rule year and stage, chosen CCN-years, holds and the years without a data year.
    cmi_files_sql = (
        "SELECT rule_fiscal_year::VARCHAR || ' ' || rule_stage || ' ' || coalesce(data_fiscal_year::VARCHAR, 'no data year'), "
        "count(DISTINCT member_sha256)::VARCHAR || ' files, ' || count(*)::VARCHAR || ' rows' FROM int_cmi_hospital_rows GROUP BY 1 ORDER BY 1;"
    )
    outcome["cmi_files"] = dict(duckdb_csv(database, cmi_files_sql))
    cmi_years_sql = (
        "SELECT rule_fiscal_year::VARCHAR || ' ' || rule_stage || ' ' || coalesce(data_fiscal_year::VARCHAR, 'no data year'), count(*)::VARCHAR "
        "FROM int_cmi_hospital_years GROUP BY 1 ORDER BY 1;"
    )
    outcome["cmi_years"] = dict(duckdb_csv(database, cmi_years_sql))
    cmi_holds_sql = "SELECT year_basis || ' ' || fiscal_year::VARCHAR || ' ' || hold_reason, count(*)::VARCHAR FROM int_cmi_holds GROUP BY 1 ORDER BY 1;"
    outcome["cmi_holds"] = dict(duckdb_csv(database, cmi_holds_sql))
    # [306] [307] One spine row per hospital with a calendar-year HAI window, counted independently from the windows.
    hai_years_sql = (
        "SELECT year(window_start)::VARCHAR, count(DISTINCT entity_id)::VARCHAR FROM int_hai_hospital_windows "
        "WHERE month(window_start) = 1 AND day(window_start) = 1 AND window_end = make_date(year(window_start), 12, 31) GROUP BY 1 ORDER BY 1;"
    )
    spine_years_sql = "SELECT window_year::VARCHAR, count(*)::VARCHAR FROM int_hospital_spine GROUP BY 1 ORDER BY 1;"
    hai_years = dict(duckdb_csv(database, hai_years_sql))
    outcome["checks"]["spine_rows_match_hai_hospitals"] = bool(hai_years) and dict(duckdb_csv(database, spine_years_sql)) == hai_years
    spine_counts_sql = (
        "SELECT window_year::VARCHAR, count(*)::VARCHAR || ' hospitals, ' || count(*) FILTER (WHERE has_pos_snapshot)::VARCHAR || ' with POS, ' "
        "|| count(*) FILTER (WHERE has_cmi)::VARCHAR || ' with CMI, ' || count(*) FILTER (WHERE is_cmi_held)::VARCHAR || ' CMI held, ' "
        "|| count(*) FILTER (WHERE is_primary_population)::VARCHAR || ' primary, ' "
        "|| count(*) FILTER (WHERE is_sensitivity_population)::VARCHAR || ' sensitivity' FROM int_hospital_spine GROUP BY 1 ORDER BY 1;"
    )
    outcome["spine"] = dict(duckdb_csv(database, spine_counts_sql))
    # [318] to [329] Care Compare windows, holds, registry coverage and Hospital General Information releases.
    cc_counts_sql = (
        "SELECT 'timely', count(*)::VARCHAR FROM int_cc_timely_effective_windows UNION ALL "
        "SELECT 'maternal', count(*)::VARCHAR FROM int_cc_maternal_windows UNION ALL "
        "SELECT 'hcahps', count(*)::VARCHAR FROM int_cc_hcahps_windows UNION ALL "
        "SELECT 'hgi rows', count(*)::VARCHAR FROM int_hgi_hospital_releases UNION ALL "
        "SELECT 'hgi release dates with two files', count(DISTINCT release_date)::VARCHAR FROM int_hgi_hospital_releases WHERE release_file_count > 1;"
    )
    outcome["care_compare_counts"] = dict(duckdb_csv(database, cc_counts_sql))
    cc_holds_sql = "SELECT bronze_table || ' ' || hold_reason, sum(row_count)::VARCHAR FROM int_cc_window_holds GROUP BY 1 ORDER BY 1;"
    outcome["care_compare_holds"] = dict(duckdb_csv(database, cc_holds_sql))
    coverage_sql = (
        "SELECT seed.measure_control, count(rows.entity_id)::VARCHAR FROM registry_measure_sources AS seed "
        "LEFT JOIN int_registry_measure_windows AS rows ON seed.measure_control = rows.measure_control "
        "WHERE seed.source_model <> 'int_hgi_hospital_releases' GROUP BY 1 ORDER BY 1;"
    )
    coverage = dict(duckdb_csv(database, coverage_sql))
    outcome["registry_controls_without_rows"] = sorted(control for control, rows in coverage.items() if rows == "0")
    outcome["checks"]["registry_seed_matches_registry"] = registry_seed_matches()
    # [331] to [340] Cost reports: reports per fiscal year, CCN-years with several reports, measure rows per control.
    cost_sql = (
        "SELECT fiscal_year::VARCHAR, count(*)::VARCHAR || ' reports, ' || count(DISTINCT ccn)::VARCHAR || ' CCNs, ' "
        "|| count(*) FILTER (WHERE reports_in_fiscal_year > 1)::VARCHAR || ' in multi-report years, ' "
        "|| count(*) FILTER (WHERE NOT is_full_year)::VARCHAR || ' not a full year' FROM int_cost_reports GROUP BY 1 ORDER BY 1;"
    )
    outcome["cost_reports"] = dict(duckdb_csv(database, cost_sql))
    cost_measure_sql = (
        "SELECT measure_control, count(*)::VARCHAR || ' rows, ' || count(coalesce(value_number::VARCHAR, value_code))::VARCHAR || ' with a value' "
        "FROM int_cost_report_measures GROUP BY 1 ORDER BY 1;"
    )
    outcome["cost_report_measures"] = dict(duckdb_csv(database, cost_measure_sql))
    outcome["checks"]["cost_seed_matches_registry"] = cost_seed_matches()
    # [342] to [353] Impact files: releases, CCNs and values per rule year; measure rows per control.
    impact_sql = (
        "SELECT rule_fiscal_year::VARCHAR, count(DISTINCT member_sha256 || ':' || sheet_name)::VARCHAR || ' releases, ' "
        "|| count(DISTINCT ccn)::VARCHAR || ' CCNs, ' || count(*)::VARCHAR || ' values, ' "
        "|| count(*) FILTER (WHERE value_text = '.')::VARCHAR || ' dots' FROM int_impact_hospital_values GROUP BY 1 ORDER BY 1;"
    )
    outcome["impact_values"] = dict(duckdb_csv(database, impact_sql))
    impact_measure_sql = (
        "SELECT measure_control || ' ' || field, count(*)::VARCHAR || ' rows, ' || min(rule_fiscal_year)::VARCHAR || '-' "
        "|| max(rule_fiscal_year)::VARCHAR FROM int_impact_measures GROUP BY 1 ORDER BY 1;"
    )
    outcome["impact_measures"] = dict(duckdb_csv(database, impact_measure_sql))
    outcome["impact_holds"] = duckdb_csv(database, "SELECT rule_fiscal_year::VARCHAR, ccn, hold_reason, hold_rows::VARCHAR FROM int_impact_holds ORDER BY ALL;")
    outcome["checks"]["impact_seed_matches_registry"] = impact_seed_matches()
    # [355] to [363] Medicare inpatient: providers and DRG cells per data year; measure rows per control.
    mup_sql = (
        "SELECT p.data_year::VARCHAR, count(*)::VARCHAR || ' providers, ' || count(*) FILTER (WHERE p.tot_benes IS NULL)::VARCHAR "
        "|| ' without totals, ' || coalesce(max(d.cells), 0)::VARCHAR || ' DRG cells' FROM int_mup_providers AS p LEFT JOIN "
        "(SELECT data_year, count(*) AS cells FROM int_mup_drg_discharges GROUP BY 1) AS d ON p.data_year = d.data_year GROUP BY 1 ORDER BY 1;"
    )
    outcome["mup_providers"] = dict(duckdb_csv(database, mup_sql))
    mup_measure_sql = (
        "SELECT measure_control, count(*)::VARCHAR || ' rows, ' || min(data_year)::VARCHAR || '-' || max(data_year)::VARCHAR "
        "FROM int_mup_measures GROUP BY 1 ORDER BY 1;"
    )
    outcome["mup_measures"] = dict(duckdb_csv(database, mup_measure_sql))
    outcome["checks"]["mup_seed_matches_registry"] = mup_seed_matches()
    # [362] Every data year with DRG cells has sepsis shares, whichever release its provider file came from.
    sepsis_gap_sql = (
        "SELECT count(*)::VARCHAR FROM (SELECT DISTINCT data_year FROM int_mup_drg_discharges) AS d WHERE NOT EXISTS "
        "(SELECT 1 FROM int_mup_measures AS m WHERE m.measure_control = 'C117' AND m.data_year = d.data_year);"
    )
    outcome["checks"]["mup_sepsis_share_every_drg_year"] = duckdb_csv(database, sepsis_gap_sql) == [["0"]]
    # [365] to [373] Ownership: the period seed reproduced; files, rows and events per table; the measure seed matches.
    outcome["checks"]["ownership_periods_seed_reproduced"] = ownership_release_periods.SEED.read_text() == ownership_release_periods.as_csv(
        ownership_release_periods.build()
    )
    ownership_sql = (
        "SELECT 'owners', count(DISTINCT member_sha256)::VARCHAR || ' files, ' || count(*)::VARCHAR || ' rows, ' "
        "|| count(*) FILTER (WHERE flags_published)::VARCHAR || ' in the flag layout, ' "
        "|| count(*) FILTER (WHERE private_equity_company_owner)::VARCHAR || ' private equity' FROM int_hospital_owner_rows "
        "UNION ALL SELECT 'enrollments', count(DISTINCT member_sha256)::VARCHAR || ' files, ' || count(*)::VARCHAR || ' rows, ' "
        "|| count(*) FILTER (WHERE ccn IS NULL)::VARCHAR || ' without a CCN, ' || count(*) FILTER (WHERE ccn <> ccn_published)::VARCHAR "
        "|| ' padded' FROM int_hospital_enrollment_rows "
        "UNION ALL SELECT 'changes_of_ownership', count(DISTINCT member_sha256)::VARCHAR || ' files, ' || count(*)::VARCHAR || ' rows, ' "
        "|| count(DISTINCT event_key)::VARCHAR || ' events, ' || count(*) FILTER (WHERE ccn_buyer IS NULL OR ccn_seller IS NULL)::VARCHAR "
        "|| ' with a value that is not a CCN' FROM int_change_of_ownership_rows;"
    )
    outcome["ownership"] = dict(duckdb_csv(database, ownership_sql))
    outcome["checks"]["ownership_seed_matches_registry"] = ownership_seed_matches()
    # [375] to [384] HHS and ONC: rows, hospitals, weeks and suppressed cells; the measure seed against the registry, with
    # each HHS child's columns derived from the stored columns.json.
    hhs_onc_sql = (
        "SELECT 'hhs', count(*)::VARCHAR || ' rows, ' || count(DISTINCT hospital_pk)::VARCHAR || ' hospitals, ' "
        "|| count(DISTINCT collection_week)::VARCHAR || ' weeks, ' || count(*) FILTER (WHERE ccn IS NULL)::VARCHAR || ' without a CCN, ' "
        "|| sum(len(suppressed_fields))::VARCHAR || ' suppressed cells, ' || count(*) FILTER (WHERE is_corrected)::VARCHAR || ' corrected' "
        "FROM int_hhs_capacity_weeks "
        "UNION ALL SELECT 'onc_chpl_linkage', count(*)::VARCHAR || ' rows, ' || count(DISTINCT ccn)::VARCHAR || ' hospitals, ' "
        "|| count(*) FILTER (WHERE meets_criteria_for_promoting_interoperability_of_ehrs IS NULL)::VARCHAR || ' blank criterion, ' "
        "|| min(program_year)::VARCHAR || '-' || max(program_year)::VARCHAR FROM int_onc_chpl_linkage_rows "
        "UNION ALL SELECT 'onc_attestations', count(*)::VARCHAR || ' rows, ' || count(DISTINCT ccn)::VARCHAR || ' hospitals, ' "
        "|| min(program_year)::VARCHAR || '-' || max(program_year)::VARCHAR FROM int_onc_attestation_rows;"
    )
    outcome["hhs_onc"] = dict(duckdb_csv(database, hhs_onc_sql))
    columns_sql = (
        "SELECT line_text FROM lakehouse.bronze.hhs_capacity_documents_text WHERE _member_sha256 = "
        "(SELECT min(_member_sha256) FROM lakehouse.bronze.hhs_capacity_documents_text WHERE _member_path = 'columns.json') ORDER BY _row_number;"
    )
    metadata = json.loads("\n".join(row[0] for row in duckdb_csv(database, columns_sql, init)))
    field_names = {item["fieldName"]: re.sub(r"[^0-9a-z]+", "_", item["name"].strip().lower()).strip("_") for item in metadata}
    outcome["checks"]["hhs_onc_seed_matches_registry"] = hhs_onc_seed_matches(field_names)
    outcome["occmix_periods"] = duckdb_csv(
        database,
        "SELECT year(survey_end_date)::VARCHAR, is_deleted::VARCHAR, count(*)::VARCHAR, count(DISTINCT member_sha256)::VARCHAR "
        "FROM int_occmix_survey_rows GROUP BY ALL ORDER BY ALL;",
    )
    outcome["occmix_holds"] = duckdb_csv(database, "SELECT hold_reason, count(*)::VARCHAR FROM int_occmix_survey_rows GROUP BY ALL ORDER BY ALL;")
    outcome["occmix_measures"] = duckdb_csv(
        database, "SELECT measure_control, count(*)::VARCHAR, count(value_number)::VARCHAR FROM int_occmix_measures GROUP BY ALL ORDER BY ALL;"
    )
    occmix_counts = duckdb_csv(
        database,
        "SELECT (SELECT count(*) FROM int_occmix_survey_rows)::VARCHAR, count(*)::VARCHAR "
        "FROM int_occmix_file_rows r JOIN int_occmix_file_layouts l USING (bronze_table, member_sha256, sheet_name) "
        "WHERE l.is_survey_layout AND l.repeated_fields = 0 AND r.source_row_number > l.header_row_number;",
    )[0]
    outcome["checks"]["occmix_source_rows_reconcile"] = int(occmix_counts[0]) > 0 and occmix_counts[0] == occmix_counts[1]
    bronze = unprefixed(duckdb_csv(database, per_table(BRONZE_COUNTS_SQL, "lakehouse.bronze."), init), "lakehouse.bronze.")
    # Bronze loads one copy per file, so its objects equal the distinct files; the copies table lists every copy [204] [206].
    model_sql = (
        "SELECT f.bronze_table, count(*)::VARCHAR, count(*)::VARCHAR, sum(f.row_count)::VARCHAR, sum(f.copy_count)::VARCHAR "
        "FROM stg_bronze__files f GROUP BY 1 ORDER BY 1;"
    )
    models = {row[0]: row[1:] for row in duckdb_csv(database, model_sql)}
    copies_sql = "SELECT table_name, count(*)::VARCHAR FROM lakehouse.bronze.stored_copies GROUP BY 1 ORDER BY 1;"
    stored_copies = dict(duckdb_csv(database, copies_sql, init))
    views = {table: values[0] for table, values in unprefixed(duckdb_csv(database, per_table(VIEW_ROWS_SQL, "stg_"), init), "stg_").items()}
    for table in TABLES:
        objects, distinct, rows = bronze[table]
        copies = stored_copies.get(table, "0")
        outcome["counts"][table] = {
            "objects": int(objects),
            "distinct_files": int(distinct),
            "distinct_rows": int(rows),
            "copies": int(copies),
            "view_rows": int(views[table]),
        }
        outcome["checks"][f"{table}_reconciles"] = models.get(table) == [objects, distinct, rows, copies] and views[table] == rows
    return outcome


def main() -> int:
    """Run the fixture cases (unless --real-only), then the real stage when asked, and write the report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--real", action="store_true", help="also build from the real bronze tables, twice")
    parser.add_argument(
        "--real-only", action="store_true", help="only the real builds and their checks; use when no fixture input or expectation changed [548]"
    )
    parser.add_argument(
        "--real-once", action="store_true", help="one real build, no rebuild comparison: an iteration run, not commit evidence; implies --real [695] [697]"
    )
    parser.add_argument("--workers", type=int, default=4, help="fixture cases built at once; 1 is serial; capped by memory and cores [687] [688]")
    args = parser.parse_args()
    real = args.real or args.real_only or args.real_once
    mode = "iteration" if args.real_once or args.real_only else ("full" if args.real else "fixture_only")
    dbt_at_start = dbt_tree_sha256()
    snapshot_dbt()
    catalog.up()
    install_packages()
    report: dict[str, Any] = {"started_at": datetime.now(UTC).isoformat(timespec="seconds"), "image": "hai-analytics:duckdb1.5.6-dbt1.11.15"}
    report["mode"] = mode
    report["fixture"] = {} if args.real_only else fixture_scenarios(max(1, args.workers))
    report["fixture_evidence"] = {**FIXTURE_EVIDENCE, "workers_requested": args.workers, "case_seconds": dict(sorted(CASE_SECONDS.items()))}
    if args.real_only:
        report["fixture_skipped"] = "--real-only: the fixture cases did not run; cite the last full pass for them"
    if real:
        report["real"] = real_stage(once=args.real_once)
        # The heavy run is over: free Docker's VM memory for the next one, unless another project's containers run [545] [546].
        report["real"]["docker_release"].append(catalog.release())
    # The code under test must be the code at the start: an edit during the run fails it [693].
    report["dbt_tree_sha256"] = dbt_at_start
    checks = dict(report["fixture"]) | (report["real"]["checks"] if real else {}) | {"dbt_unchanged_during_run": dbt_tree_sha256() == dbt_at_start}
    # Launch records in a fixed order: case, then launch order [694].
    BUDGETS.sort(key=lambda item: (str(item.get("case")), int(item.get("launch", 0))))
    report["passed"] = sum(checks.values())
    report["total"] = len(checks)
    results = CASES / "base/target/run_results.json"
    if results.exists():
        report["dbt_version"] = json.loads(results.read_text())["metadata"]["dbt_version"]
    REPORTS.mkdir(parents=True, exist_ok=True)
    path = REPORTS / f"report_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    for name, ok in checks.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    note = "; iteration run: not commit evidence" if mode == "iteration" else ""
    sys.stdout.write(f"staging e2e ({mode}): {report['passed']} of {report['total']} passed; report {path.relative_to(REPO_ROOT)}{note}\n")
    return 0 if report["passed"] == report["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
