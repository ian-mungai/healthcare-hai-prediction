"""E2E check of the staging copy, label, twin, sheet, POS, CMI, spine and Care Compare models (failure modes 166 to 173, 177 to 188, 267 to 341).

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
Hospital General Information value that does not cast, a cost report in the wrong file year and a cost-report amount that
does not cast.
The real stage checks that the generators reproduce the committed seeds, builds the models from the catalog twice and
reconciles them with bronze.

Failure modes: ``data/lakehouse_planning/staging_dedup_20261003/failure_modes.md``,
``data/lakehouse_planning/staging_families_20261003/failure_modes.md``,
``data/lakehouse_planning/sheet_selection_20261005/failure_modes.md``,
``data/lakehouse_planning/hospital_spine_20261005/failure_modes.md``,
``data/lakehouse_planning/group_b_20261005/failure_modes_b1.md`` and ``failure_modes_b2.md`` in the same folder. The report in ``data/e2e/staging/`` holds
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

from scripts.lakehouse import catalog, ipps_file_labels, pos_file_periods
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
    "Provider\tName\tCMI\tPayment\tShare",
    '010001\t"Alpha, Hospital "\t1.2346\t"$1,234.50"\t1.58%',
    '010002\tBeta\t0.5\t"$60,591,269.77"\t0.22%',
)
TWIN_SHEET = (
    "Variable Descriptions:Variable|Meaning",
    "Data:Provider|Name|CMI|Payment|Share",
    "Data:10001|Alpha, Hospital|1.23456789|1234.4978|0.0158371255",
    "Data:10002|Beta|0.5|60591269.765|0.0021846031",
)
OCCMIX_TEXT = ("Provider,Wage", "010001,3.5", "010002,4.0")
# The BRZ-016 text held under two names; its workbook twin agrees with it [268].
HELD_TEXT = ("Provider\tCMI", "010001\t1.5")
# A final-rule workbook whose correction-notice sheet has the text's row count but other values [267] [269].
FR_CN_SHEET = (
    "FR 2024:Provider|Name|CMI|Payment|Share",
    "FR 2024:10001|Alpha, Hospital|1.23456789|1234.4978|0.0158371255",
    "FR 2024:10002|Beta|0.5|60591269.765|0.0021846031",
    "CN 2024:Provider|Name|CMI|Payment|Share",
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
WIDE_COLUMNS = {
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
    "cms_hospital_enrollments": ("ccn", "enrollment_id"),
    "cms_hospital_owners": ("enrollment_id", "private_equity_company_owner"),
    "cms_change_of_ownership": ("ccn_buyer", "ccn_seller", "effective_date"),
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
        records=((("enrollment_id", "O20000000001"), ("private_equity_company_owner", "N")),),
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


BASE = (
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
        content=("FY19 NPRM:Provider|CMI", "FY19 NPRM:10001|1.5", "Variable Descriptions:Variable|Meaning"),
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
        2,
        content=("Data:10001|ALPHA|1.2345", "Data:10002|BETA|0.5"),
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
    Stored(
        "cms_occupational_mix_text_lines", "m05", "CMS_OCCMIX__p", "FY26_OccMix_PUF.txt", sha("p3"), 2, "CMS_OCCMIX__p", content=("PROV\tOM", "010001\t0.5")
    ),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v04",
        "CMS_OCCMIX__p",
        "FY26_S3_PUF.xlsx",
        sha("p2"),
        4,
        "CMS_OCCMIX__p",
        content=("S-3 Data:PROV|S3", "S-3 Data:010001|100", "OccMix Data:PROV|OM", "OccMix Data:010001|0.5"),
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
)
# Each failing case changes the base fixture, or drops label and period rows, and names the one dbt test that must catch it.
FAILING: dict[str, tuple[str, tuple[Stored, ...], frozenset[str]]] = {
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
TEXT_TABLES = {"cms_ipps_text_lines", "cms_occupational_mix_text_lines", "cms_occupational_mix_text_lines_utf16"}
SHEET_TABLES = {"cms_ipps_sheet_rows", "cms_occupational_mix_sheet_rows"}


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
VIEW_ROWS_SQL = "SELECT getvariable('checked_table'), count(*)::VARCHAR FROM query_table(getvariable('checked_table'));\n"
BRONZE_COUNTS_SQL = (
    "WITH o AS (SELECT _object_key, any_value(_member_sha256) AS sha, count(*) AS n FROM query_table(getvariable('checked_table')) GROUP BY 1), "
    "d AS (SELECT DISTINCT sha, n FROM o) "
    "SELECT getvariable('checked_table'), (SELECT count(*) FROM o)::VARCHAR, (SELECT count(*) FROM d)::VARCHAR, (SELECT sum(n) FROM d)::VARCHAR;\n"
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
    }


def refuses(action: Any, fragment: str) -> bool:
    """Return whether the action raises the generator's error with the fragment in its message."""
    try:
        action()
    except ipps_file_labels.LabelError as error:
        return fragment in str(error)
    return False


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
        ("p2", "OccMix Data", "covered", "p3", "false"),
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
    outcome["checks"]["real_rebuild_identical"] = builds[0] == builds[1]
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
