"""E2E check of the staging copy, label, twin, sheet, POS, CMI, spine, Care Compare and geography models (failure modes 166 to 173, 177 to 188, 267 to 419).

Run from the repository root with Docker running:

    .venv/bin/python -m scripts.lakehouse.run_staging_e2e            # fixture cases only
    .venv/bin/python -m scripts.lakehouse.run_staging_e2e --real     # then the real bronze tables, built twice

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

Failure modes: ``data/lakehouse_planning/staging_dedup_20261003/failure_modes.md``,
``data/lakehouse_planning/staging_families_20261003/failure_modes.md``,
``data/lakehouse_planning/sheet_selection_20261005/failure_modes.md``,
``data/lakehouse_planning/hospital_spine_20261005/failure_modes.md``,
``data/lakehouse_planning/group_b_20261005/failure_modes_b1.md`` to ``failure_modes_b5c.md`` in the same folder,
``data/lakehouse_planning/group_c_20261006/failure_modes_c1.md``. The report in ``data/e2e/staging/`` holds
outcomes and counts, never data values or credentials.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import shutil
import sys
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import catalog, geography_file_periods, ipps_file_labels, ownership_release_periods, pos_file_periods
from scripts.process import run_command

REPO_ROOT = catalog.REPO_ROOT
OUT = REPO_ROOT / "data/analytics/dbt"
CASES = OUT / "e2e"
REPORTS = REPO_ROOT / "data/e2e/staging"
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
    pos("010002", state_cd="AL", pgm_trmntn_cd="00", bed_cnt=" "),
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
    pos("070001", prvdr_ctgry_sbtyp_cd="01", state_cd="CT", fips_state_cd="09", fips_cnty_cd="001", pgm_trmntn_cd="00", bed_cnt="300"),
    pos("990001", state_cd="CN", pgm_trmntn_cd="00"),
    pos("210001", prvdr_ctgry_sbtyp_cd="01", state_cd="MD", fips_state_cd="24", fips_cnty_cd="005", pgm_trmntn_cd="00", bed_cnt="200"),
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
)
# Each POS file's catalog coverage, as the period seed gives it [280].
POS_PERIODS = {"pa": ("2018-10-01", "2018-12-31"), "pb": ("2019-01-01", "2019-03-31"), "pc": ("2020-10-01", "2020-12-31")}
# Each owner, enrollment and change-of-ownership file's release label and catalog period, as the receipts give them [365].
OWNERSHIP_PERIODS = {
    "e1": ("Hospital Enrollments : 2024-01-01", "2024-01-01", "2024-01-31"),
    "o1": ("Hospital All Owners : 2025-05-01", "2025-05-01", "2025-05-31"),
    "x1": ("Hospital Change of Ownership : 2023-12-01", "2023-10-01", "2023-12-31"),
    "ow1": ("Hospital All Owners : 2022-11-14", "2022-11-01", "2022-11-30"),
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
    Stored("cms_provider_of_services", "pc", "CMS_POS__fixture_c", "POS_OTHER_DEC20.csv", sha("t3"), 7, "CMS_POS__fixture_c", records=POS_DEC20),
    Stored("cms_hai_hospital", "h08", "2022-06-01", "HAI_Spine_Fixture.csv", sha("a6"), 9, content=HAI_SPINE),
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
        2,
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
        3,
        records=(
            HHS_WEEK,
            cc(hospital_pk="010001", collection_week="2021/01/10", ccn="010001", state="AL", is_corrected="true", total_beds_7_day_avg="251"),
            cc(hospital_pk="3f" * 32, collection_week="2021/01/03", state="AL", total_beds_7_day_avg="40"),
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
TEXT_TABLES = {"cms_ipps_text_lines", "cms_occupational_mix_text_lines", "cms_occupational_mix_text_lines_utf16", "county_adjacency_2010_text_lines"}
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
            # HAI content fills the HAI columns under the newer layout's names; empty parts stay null.
            hai = {"facility_id": "", "state": "", "measure_id": "", "start_date": "", "end_date": "", "score": ""}
            if item.table in HAI_TABLES and item.content:
                entity, measure, start, end, score = text.split("|")
                entity_column = HAI_TABLES[item.table]
                if entity_column:
                    hai[entity_column] = entity
                hai.update(measure_id=measure, start_date=start, end_date=end, score=score)
            writer.writerow(
                (item.table, item.key, "fixture", item.snapshot, "fixture_dataset", item.release, f"fixture/{item.key}.zip", f"v-{item.key}", item.member)
                + (checksum, row, value, text, sheet, cells)
                + (hai["facility_id"], "", hai["state"], hai["measure_id"], hai["start_date"], hai["end_date"], "", "", hai["score"], "", "")
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


def geography_periods_csv(objects: Iterable[Stored], unlabelled: frozenset[str]) -> str:
    """Return the geography period seed for the fixture's files, written by the real generator, without the ones a case leaves out [403]."""
    loaded = [
        {"table": item.table, "sha256": item.sha, "release_id": item.release, "file_name": item.member}
        for item in objects
        if item.table in geography_file_periods.TABLES and item.key not in unlabelled
    ]
    return geography_file_periods.as_csv(geography_file_periods.rows_for(loaded, GEOGRAPHY_QUARTERS, GEOGRAPHY_COVERAGE))


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
SPINE_SQL = (
    "SELECT ccn, window_year::VARCHAR, coalesce(pos_period_end::VARCHAR, ''), coalesce(state_code, ''), coalesce(county_fips, ''), "
    "coalesce(cmi::VARCHAR, ''), coalesce(cmi_data_fiscal_year::VARCHAR, ''), coalesce(cmi_rule_fiscal_year::VARCHAR, ''), "
    "coalesce(cmi_rule_stage, ''), has_pos_snapshot::VARCHAR, has_cmi::VARCHAR, is_cmi_held::VARCHAR, is_critical_access::VARCHAR, "
    "is_veterans_affairs::VARCHAR, is_state_or_dc::VARCHAR, is_connecticut::VARCHAR, is_primary_population::VARCHAR, "
    "is_sensitivity_population::VARCHAR FROM int_hospital_spine ORDER BY ALL;"
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


def per_table(query: str, prefix: str) -> str:
    """Return the query once per table, each run after setting the checked_table variable to the prefixed table name."""
    return "".join(f"SET VARIABLE checked_table = '{prefix}{table}';\n{query}" for table in TABLES)


def unprefixed(rows: list[list[str]], prefix: str) -> dict[str, list[str]]:
    """Key query rows by table name without its prefix."""
    return {row[0].removeprefix(prefix): row[1:] for row in rows}


def compose_run(service_args: list[str], extra_env: dict[str, str]) -> tuple[int, str, str]:
    """Run one container of the analytics-dbt service and return its exit code and output."""
    env_flags = [flag for name, value in extra_env.items() for flag in ("-e", f"{name}={value}")]
    args = ["compose", "--project-directory", str(REPO_ROOT), "-f", str(REPO_ROOT / "docker-compose.yaml"), "--env-file", str(catalog.COMPOSE_ENV)]
    args += ["--profile", "query", "run", "--rm", "-T", "--quiet-pull", *env_flags, *service_args]
    result = run_command("docker", args, cwd=REPO_ROOT, env=catalog.system_environment(), timeout=TIMEOUT)
    return result.returncode, result.stdout, result.stderr


def duckdb_csv(database: str, sql: str, init: str | None = None) -> list[list[str]]:
    """Query a database in the case folder with the DuckDB CLI and return the rows without the header."""
    init_flags = ["-init", init] if init else []
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
    shutil.copytree(REPO_ROOT / "dbt", project)
    stored = generator_objects(objects)
    rows = [row for row in ipps_file_labels.labels(stored, {}) if row["object_key"] not in unlabelled]
    (project / "seeds/ipps_occmix_copy_labels.csv").write_text(ipps_file_labels.as_csv(rows, ipps_file_labels.LABEL_COLUMNS))
    twin_rows = ipps_file_labels.twins(stored, FIXTURE_RENAMED)
    (project / "seeds/ipps_occmix_twins.csv").write_text(ipps_file_labels.as_csv(twin_rows, ipps_file_labels.TWIN_COLUMNS))
    (project / "seeds/pos_file_periods.csv").write_text(periods_csv(objects, unlabelled))
    (project / "seeds/ownership_release_periods.csv").write_text(ownership_periods_csv(objects, unlabelled))
    (project / "seeds/geography_file_periods.csv").write_text(geography_periods_csv(objects, unlabelled))


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
    (case_dir / "bronze.sql").write_text(variables + FIXTURE_SQL)
    database = f"{CONTAINER_OUT}/e2e/{case}/fixture_lakehouse.duckdb"
    code, _, stderr = compose_run(["--entrypoint", "duckdb", "analytics-dbt", database, "-c", f".read {CONTAINER_OUT}/e2e/{case}/bronze.sql"], {})
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
        "chow": [tuple(row) for row in duckdb_csv(database, CHOW_SQL)],
        "hhs": [tuple(row) for row in duckdb_csv(database, HHS_SQL)],
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


def fixture_scenarios() -> dict[str, bool]:
    """Run every fixture case and return each check's outcome."""
    checks: dict[str, bool] = {}
    want = expected(BASE)
    code, statuses = run_fixture("base", BASE)
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
            ("hospital", "010005", "HAI_1_SIR", "2020-04-01", "2021-03-31", "0.7", "a6"),
            ("hospital", "010005", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.6", "a6"),
            ("hospital", "010009", "HAI_1_SIR", "2021-01-01", "2021-12-31", "1.1", "a6"),
            ("hospital", "011301", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.5", "a6"),
            ("hospital", "01001F", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.3", "a6"),
            ("hospital", "070001", "HAI_1_SIR", "2021-01-01", "2021-12-31", "0.9", "a6"),
            ("hospital", "990001", "HAI_1_SIR", "2021-01-01", "2021-12-31", "1.0", "a6"),
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
    ]
    # [280] to [292] Hospital rows only, one per CCN and period; padded codes, 5-character counties, typed counts and
    # switches, dates, and the population flags.
    checks["pos_snapshots_match_expected"] = base.get("pos") == sorted(
        [
            ("010001", "2018-12-31", "AL", "01073", "01360", "01", "04", "00", "true", "250", "240", "100.5", "true", "false", "1", "1966-07-01")
            + ("", "35233", "true", "false", "false", "false"),
            ("010001", "2019-03-31", "AL", "01073", "", "01", "", "00", "true", "255", "", "101.0", "true", "false", "", "")
            + ("", "", "true", "false", "false", "false"),
            ("010002", "2018-12-31", "AL", "", "", "", "", "00", "true", "", "", "", "", "", "", "") + ("", "", "true", "false", "false", "false"),
            ("010002", "2019-03-31", "AL", "", "", "", "", "01", "false", "50", "", "", "", "", "", "") + ("2019-01-15", "", "true", "false", "false", "false"),
            ("01001F", "2018-12-31", "AL", "01001", "", "", "10", "00", "true", "100", "", "", "", "", "", "") + ("", "", "true", "false", "false", "true"),
            ("011301", "2018-12-31", "AL", "01003", "", "11", "", "00", "true", "25", "", "", "", "", "", "") + ("", "", "true", "false", "true", "false"),
            ("070001", "2018-12-31", "CT", "09001", "", "", "", "00", "true", "", "", "", "", "", "", "") + ("", "", "true", "true", "false", "false"),
            ("990001", "2018-12-31", "CN", "", "", "", "", "00", "true", "", "", "", "", "", "", "") + ("", "", "false", "false", "false", "false"),
            ("010001", "2020-12-31", "AL", "01073", "", "01", "", "00", "true", "260", "", "", "", "", "", "") + ("", "", "true", "false", "false", "false"),
            ("010005", "2020-12-31", "AL", "01089", "", "01", "", "00", "true", "120", "", "", "", "", "", "") + ("", "", "true", "false", "false", "false"),
            ("011301", "2020-12-31", "AL", "01003", "", "11", "", "00", "true", "25", "", "", "", "", "", "") + ("", "", "true", "false", "true", "false"),
            ("01001F", "2020-12-31", "AL", "01001", "", "", "", "00", "true", "100", "", "", "", "", "", "") + ("", "", "true", "false", "false", "true"),
            ("070001", "2020-12-31", "CT", "09001", "", "01", "", "00", "true", "300", "", "", "", "", "", "") + ("", "", "true", "true", "false", "false"),
            ("990001", "2020-12-31", "CN", "", "", "", "", "00", "true", "", "", "", "", "", "", "") + ("", "", "false", "false", "false", "false"),
            ("210001", "2020-12-31", "MD", "24005", "", "01", "", "00", "true", "200", "", "", "", "", "", "") + ("", "", "true", "false", "false", "false"),
        ]
    )
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
    # 12 months before the window, the CMI of the fiscal year before it, and the population flags.
    no_pos = ("", "", "", "")
    no_cmi = ("", "", "", "")
    checks["spine_matches_expected"] = base.get("spine") == sorted(
        [
            ("010001", "2019", "2018-12-31", "AL", "01073", "1.9186", "2018", "2020", "final")
            + ("true", "true", "false", "false", "false", "true", "false", "true", "true"),
            ("010002", "2019", "2018-12-31", "AL", "") + no_cmi + ("true", "false", "false", "false", "false", "true", "false", "false", "false"),
            ("010001", "2021", "2020-12-31", "AL", "01073", "2.0352", "2020", "2023", "proposed")
            + ("true", "true", "false", "false", "false", "true", "false", "true", "true"),
            ("010005", "2021", "2020-12-31", "AL", "01089") + no_cmi + ("true", "false", "true", "false", "false", "true", "false", "false", "false"),
            ("010009", "2021")
            + no_pos[:3]
            + ("1.2", "2020", "2023", "proposed")
            + ("false", "true", "false", "false", "false", "false", "false", "false", "false"),
            ("011301", "2021", "2020-12-31", "AL", "01003") + no_cmi + ("true", "false", "false", "true", "false", "true", "false", "false", "true"),
            ("01001F", "2021", "2020-12-31", "AL", "01001") + no_cmi + ("true", "false", "false", "false", "true", "true", "false", "false", "false"),
            ("070001", "2021", "2020-12-31", "CT", "09001", "1.5", "2020", "2023", "proposed")
            + ("true", "true", "false", "false", "false", "true", "true", "true", "true"),
            ("990001", "2021", "2020-12-31", "CN", "") + no_cmi + ("true", "false", "false", "false", "false", "false", "false", "false", "false"),
            # Maryland: a CMI, but out of the primary population and in the sensitivity run (owner decision, Oct 5 2026) [317].
            ("210001", "2021", "2020-12-31", "MD", "24005", "1.8", "2020", "2023", "proposed")
            + ("true", "true", "false", "false", "false", "true", "false", "false", "true"),
            ("010001", "2025") + no_pos[:3] + no_cmi + ("false", "false", "false", "false", "false", "false", "false", "false", "false"),
            ("01000F", "2025") + no_pos[:3] + no_cmi + ("false", "false", "false", "false", "true", "false", "false", "false", "false"),
        ]
    )
    # [318] to [322] Care Compare windows as for HAI: the latest release wins, conflicts and unparsed dates are held.
    checks["timely_windows_match_expected"] = base.get("timely") == [
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
    ]
    # [323] [325] [326] [327] Registry controls get exactly their named measure; numbers only for plain numbers.
    checks["registry_windows_match_expected"] = base.get("registry") == [
        ("C119", "010001", "2022-01-01", "80", "80.0", ""),
        ("C139", "010001", "2022-01-01", "21", "21.0", ""),
        ("C140", "010001", "2022-01-01", "507", "507.0", ""),
        ("C141", "010002", "2022-01-01", "high", "", ""),
        ("C143", "010001", "2022-01-01", "152", "152.0", ""),
        ("C167", "010001", "2023-01-01", "Yes", "", ""),
        ("C168", "010001", "2023-01-01", "30", "30.0", ""),
    ]
    # [328] [329] One Hospital General Information row per CCN and file; two files on one release date are both kept.
    checks["hgi_releases_match_expected"] = base.get("hgi") == [
        ("010001", "2024-01-31", "2", "Acute Care Hospitals", "true", "3", "3", "", "u5"),
        ("010001", "2024-01-31", "2", "Acute Care Hospitals", "true", "4", "4", "", "u1"),
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
        ("u3", "2025-05-31", "O20000000001", "", "", "", "", "true", "false", "", "", ""),
    ]
    checks["enrollments_match_expected"] = base.get("enrollments") == [
        ("u2", "2024-01-31", "O20000000001", "010001", "010001", "", "", "", ""),
        ("v1", "2022-11-30", "O20000000002", "013025", "13025", "P", "1990-01-05", "true", "false"),
        ("v1", "2022-11-30", "O20000000003", "01T001", "01T001", "", "", "false", ""),
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
    checks["hhs_match_expected"] = base.get("hhs") == [
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
        ("3f3f3f", "2021-01-03", "", "", "40.0", "", "", "", "", ""),
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
    code, _ = run_fixture("base_again", BASE)
    checks["rebuild_identical"] = code == 0 and "error" not in base and model_outputs("base_again") == base
    code, _ = run_fixture("reversed_order", tuple(reversed(BASE)))
    checks["load_order_independent"] = code == 0 and "error" not in base and model_outputs("reversed_order") == base
    for case, (test, objects, unlabelled) in FAILING.items():
        code, statuses = run_fixture(case, objects, unlabelled)
        checks[f"{case}_fails_{test}"] = code != 0 and statuses.get(test) == "fail"
    return checks


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


def mup_seed_matches() -> bool:
    """Check that the Medicare inpatient measure seed covers exactly the registry's controls of its two sources [363]."""
    registry = json.loads((REPO_ROOT / "config/acquisition/source_registry.json").read_text())
    sources = {source["source_id"]: source for source in registry["sources"]}
    with (REPO_ROOT / "dbt/seeds/mup_measures.csv").open(newline="") as handle:
        seed = sorted({row["measure_control"] for row in csv.DictReader(handle)})
    expected = {*sources["CMS-MUP-PROVIDER"]["linked_measure_ids"], *sources["CMS_MEDICARE_PROVIDER"]["linked_measure_ids"]}
    return seed == sorted(expected)


# Ordered fingerprints of every C1 model, compared between the two real builds [418].
GEOGRAPHY_FINGERPRINTS = {
    "int_hud_zip_county_quarters": "SELECT count(*)::VARCHAR, md5(string_agg(to_json(t), chr(10) ORDER BY hud_row_key)) FROM int_hud_zip_county_quarters AS t;",
    "int_hud_zip_county_holds": "SELECT count(*)::VARCHAR, md5(string_agg(to_json(t), chr(10) ORDER BY hud_row_key)) FROM int_hud_zip_county_holds AS t;",
    "int_county_adjacency_edges": "SELECT count(*)::VARCHAR, md5(string_agg(to_json(t), chr(10) ORDER BY edge_key)) FROM int_county_adjacency_edges AS t;",
    "int_rucc_county_codes": "SELECT count(*)::VARCHAR, md5(string_agg(to_json(t), chr(10) ORDER BY rucc_key)) FROM int_rucc_county_codes AS t;",
    "int_ruca_codes": "SELECT count(*)::VARCHAR, md5(string_agg(to_json(t), chr(10) ORDER BY ruca_row_key)) FROM int_ruca_codes AS t;",
    "int_hsa_zip_cases": "SELECT count(*)::VARCHAR, md5(string_agg(to_json(t), chr(10) ORDER BY hsa_row_key)) FROM int_hsa_zip_cases AS t;",
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


def real_stage() -> dict[str, Any]:
    """Build the models from the catalog twice and reconcile them with bronze."""
    outcome: dict[str, Any] = {"checks": {}, "counts": {}}
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
    for run in ("real", "real_again"):
        (CASES / run).mkdir(parents=True, exist_ok=True)
        code, statuses = dbt_build(run, "lakehouse")
        failed = sorted(name for name, status in statuses.items() if status not in ("pass", "success"))
        outcome["checks"][f"{run}_build_passes"] = code == 0 and bool(statuses) and not failed
        outcome[f"{run}_not_passing"] = failed
        builds.append([row[:6] for row in duckdb_csv(database, FILES_SQL)])
        outcome[f"{run}_occmix_fingerprint"] = duckdb_csv(
            database,
            "SELECT count(*)::VARCHAR, md5(string_agg(to_json(s), chr(10) ORDER BY survey_row_key)) FROM int_occmix_survey_rows AS s;",
        )
        outcome[f"{run}_geography_fingerprints"] = {model: duckdb_csv(database, query) for model, query in GEOGRAPHY_FINGERPRINTS.items()}
    outcome["checks"]["real_rebuild_identical"] = builds[0] == builds[1]
    outcome["checks"]["occmix_real_rebuild_identical"] = outcome["real_occmix_fingerprint"] == outcome["real_again_occmix_fingerprint"]
    outcome["checks"]["geography_real_rebuild_identical"] = outcome["real_geography_fingerprints"] == outcome["real_again_geography_fingerprints"]
    geography = geography_real(database, init)
    outcome["checks"].update(geography["checks"])
    outcome["geography_counts"] = geography["counts"]
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
    """Run the fixture cases, then the real stage when asked, and write the report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--real", action="store_true", help="also build from the real bronze tables, twice")
    args = parser.parse_args()
    catalog.up()
    install_packages()
    report: dict[str, Any] = {"started_at": datetime.now(UTC).isoformat(timespec="seconds"), "image": "hai-analytics:duckdb1.5.6-dbt1.11.15"}
    report["fixture"] = fixture_scenarios()
    if args.real:
        report["real"] = real_stage()
    checks = dict(report["fixture"]) | (report["real"]["checks"] if args.real else {})
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
    sys.stdout.write(f"staging e2e: {report['passed']} of {report['total']} passed; report {path.relative_to(REPO_ROOT)}\n")
    return 0 if report["passed"] == report["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
