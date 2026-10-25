---
title: Staging Models
description: What each staging and intermediate model keeps, holds and maps, and the coverage limits of the staging layer.
last_updated: 2026-10-13
---

# Staging Models

The dbt staging layer (`dbt/`, dbt-core with dbt-duckdb in the analytics image) reads bronze read-only. Its E2E builds
the models on synthetic fixtures, including cases that must fail one named test, then on the real bronze tables twice;
it reconciles each staging table with bronze. Before each real build it pins the current snapshot of every mapped
bronze table in a new run folder and verifies after the build that none moved (`scripts/lakehouse/pins.py`, run in the
Spark container as `catalog job pins`); a moved table fails the run. Where a text file and a workbook hold the same IPPS or occupational-mix
table, staging reads the text file and drops only the workbook sheets that a selected text file of the same release
matches row for row; every other sheet stays selected (`stg_bronze__sheet_selection`). `scripts.lakehouse.ipps_file_labels` regenerates the IPPS and
occupational-mix label and twin seeds from the S3 manifests; `--check` confirms the committed seeds. `int_pos_hospital_snapshots`
types the Provider of Services hospital rows; each file is dated by the catalog coverage in `dbt/seeds/pos_file_periods.csv`,
which `scripts.lakehouse.pos_file_periods` rebuilds from the S3 manifests and the local acquisition job plans. A valid
published county is kept (`county_source` pos). A county that is missing or outside the snapshot's state takes the county of
the snapshot's ZIP when the HUD quarter in force on the snapshot's period end lists that ZIP, with a residential ratio
above 0, in exactly one county under the snapshot's state (`county_source` hud_zip_crosswalk). Connecticut fills only from
090xx county codes and keeps a published 091xx county. Otherwise the county stays null and `county_fill_reason` says why;
`county_fips_published` keeps the published value. The case-mix index
models read the unadjusted CMI by exact header name (`dbt/seeds/cmi_layout_columns.csv`) and keep the transfer-adjusted CMI in its
own column, with one row per CCN in each selected file and each rule year and stage (`int_cmi_hospital_rows`). Choosing one CMI
per hospital and rule year is saved for the feature phase (see [Saved for the Feature Phase](feature_phase_saved.md)).
The Care Compare timely and effective
care, maternal health and HCAHPS tables get the same window models as HAI (`int_cc_*_windows`, holds in
`int_cc_window_holds`). `int_hgi_hospital_releases` types Hospital General Information per file.
`int_cost_reports` has one row per hospital cost report, in the federal fiscal year in which its period starts, with the
number of reports of its CCN in that year. The IPPS
impact files from FY 2001 are read by exact header name (`dbt/seeds/impact_layout_columns.csv`): `int_impact_hospital_values`
has one row per release, hospital and field. The
Medicare inpatient provider and DRG summaries are typed per file, with the data and release years from the file name
(`int_mup_providers`, `int_mup_drg_discharges`); a blank, suppressed cell is null. The registry control mappings are the gold
control bridge seeds `dbt/seeds/gold/<name>_bridge.csv`: the registry-map seeds that sat beside the staging seeds in
`dbt/seeds/` are retired. Each bridge keeps the stem of its retired seed plus `_bridge`.
`scripts/lakehouse/bridge_references.py --check` checks the bridges against `data_contracts/lakehouse/gold_contract.json`. The
formula rules are in `dbt/seeds/features/<name>_recipe.csv`, for the cost report, IPPS impact, Medicare inpatient provider,
occupational-mix, HHS and ONC, MMD and shortage families; the other families have no recipe. The Care Compare measures
of the window models above map by exact measure ID in `registry_measure_sources_bridge.csv` and the registry's HAI controls
(C269 to C283 and C283's children) map to the published HAI parts in `hai_outcome_measures_bridge.csv`. The cost-report
measures map in `cost_report_measures_bridge.csv`, the registry measures of source CMS_IPPS in `impact_measures_bridge.csv`
and those of sources CMS-MUP-PROVIDER and CMS_MEDICARE_PROVIDER in `mup_measures_bridge.csv`. The
owner, enrollment and change-of-ownership files are typed per release (`int_hospital_owner_rows`,
`int_hospital_enrollment_rows`, `int_change_of_ownership_rows`). Each file is dated by the publisher's catalog period in
`dbt/seeds/ownership_release_periods.csv`, which `scripts.lakehouse.ownership_release_periods` rebuilds from the S3
manifests and the local capture receipts. A change of ownership listed in several releases is kept once, from the latest
release (the latest catalog period end, then the highest release ID, then the lower source row and checksum). Another
release whose published values differ is held in `int_change_of_ownership_holds`, as is a repeated row of the event in the
kept release; values are compared as published, so associate IDs 123 and 0123 differ. Associate IDs
that lost leading zeros are padded to 10 digits and the row is marked. An owner flag a release's layout lacks (private equity and REIT before
April 2025) is null; `dbt/seeds/gold/ownership_measures_bridge.csv` names the columns each registry control uses. The HHS weekly
hospital capacity rows are typed per hospital and week (`int_hhs_capacity_weeks`): a count of 1 to 3, published as
-999999, is null and listed in `suppressed_fields`; any other negative value is null and listed in `negative_fields`.
The ONC Promoting Interoperability to certified-product linkage and
the older 2011 to 2017 EHR incentive attestations are typed apart (`int_onc_chpl_linkage_rows`,
`int_onc_attestation_rows`). `dbt/seeds/gold/hhs_onc_measures_bridge.csv` maps the registry controls, with the formula rules in
`dbt/seeds/features/hhs_onc_measures_recipe.csv`; the HHS field names the
source truncates are resolved through the stored `columns.json`.

Occupational-mix surveys are typed per file, sheet and source row (`int_occmix_survey_rows`). Exact headers distinguish
survey records from CBSA and S-3 tables; the layout inventory retains every selected file and sheet. Survey dates come
from FROM and TO, including workbook ISO dates. Payment-rule years stay separate. Deleted records, invalid
identifiers or periods and duplicate provider-period rows remain visible with a hold and no derived values.
`dbt/seeds/gold/occmix_measures_bridge.csv` maps all six S03 registry controls (rules in
`dbt/seeds/features/occmix_measures_recipe.csv`): RN paid hours, the RN, combined LPN/LVN plus
surgical-technologist and NAORAT paid-hour shares and RN salary per paid hour. Missing values and nonpositive
denominators give null. C043 stays unavailable: paid hours cannot substitute for direct-care worked hours per patient-day;
registry revision 3 closes it as unavailable from public sources.
Comma-grouped numbers are parsed only when their grouping is valid. Dots, dashes and blanks remain null.
Source revisions remain separate; hospital-window alignment, publication timing and model eligibility are pending.

The geography sources are typed at their own grain. HUD ZIP-to-county rows carry the USPS quarter their capture
receipt records (`int_hud_zip_county_quarters`), with each address ratio kept separately; the 47 rows whose code is
not a county are held apart (`int_hud_zip_county_holds`). County adjacency covers the 2010 and 2023 to 2026 files with
self-links and islands flagged (`int_county_adjacency_edges`). Rural-Urban Continuum Codes (2013, 2023) and Rural-Urban
Commuting Area codes (2010, 2020; tracts and ZIP codes) stay text categories (`int_rucc_county_codes`,
`int_ruca_codes`). Hospital service area counts keep their suppression marks as flags (`int_hsa_zip_cases`). County
codes are 5-character text; Connecticut is flagged, not removed. `dbt/seeds/geography_file_periods.csv`, generated by
`scripts/lakehouse/geography_file_periods.py`, dates every file: HUD and service-area files from their acquisition
records, RUCC, RUCA and adjacency files by a reviewed vintage that is not a publication date.
`dbt/seeds/gold/geography_measures_bridge.csv` maps the eight registry controls of these families. Weights, graph rules and
hospital-window alignment are pending.

ACS 5-year values for 2010 to 2024 come from the data.census.gov exports and, for the detailed tables from 2018, the
summary files (`int_acs_county_values`, one row per county, vintage and concept). ACS column codes change meaning between
vintages, so `dbt/seeds/acs_variable_map.csv` names, per concept and vintage, the column and the exact label it must carry;
a test fails when a file's label differs. A wording change of meaning holds the vintage instead of mixing it in; so do
the subject-table units that changed from percents to counts in 2017. Missing marks and sentinels stay as tokens; a capped
median keeps its cap and a flag. SVI's seven editions are staged long under each edition's own field names
(`int_svi_county_values`), with -999 kept as a token. The ACS file vintage and the SVI edition come from
`dbt/seeds/geography_file_periods.csv`. `dbt/seeds/gold/acs_svi_measures_bridge.csv` maps 43 of the 44 S18 and S19 controls to ACS concepts
and SVI fields (C177.02 is in `dbt/seeds/gold/income_labor_measures_bridge.csv`); staging picks no source between them.

SAIPE county estimates for 1989 to 2024 are cut from the fixed-width files at the positions every year's layout documents
(`int_saipe_county_estimates`): poverty counts and percents with 90% bounds and median household income in nominal dollars.
SAHIE county rows for 2006 to 2024 keep their published age, race, sex and income groups (`int_sahie_county_rows`); a test
checks that each file's preamble defines code 0 as under 65, all races, both sexes and all incomes, the group the registry
names. BLS LAUS county series (`int_bls_county_series`) keep months and annual averages apart, with footnote codes and the
capture date as the revision vintage. `.` and `-` stay as missing marks. `dbt/seeds/gold/income_labor_measures_bridge.csv` maps the
SAIPE, SAHIE and BLS registry controls.

PLACES county values keep their release, data year, measure and crude or age-adjusted type (`int_places_county_values`);
`is_all_states` marks the measure-years whose county rows cover all 50 states and DC, the only ones the project
uses. The 2020 release has no county codes; its rows take the code PLACES publishes for the same state and county name
in its other releases (`county_source`). Medicare Geographic Variation county values are staged
long for every published age level with `*` and `NA` kept as tokens (`int_gv_county_values`). WONDER deaths and crude rates keep their
database, bridged race (1999 to 2020) or single race (2018 to 2024), with their Suppressed, Unreliable, Missing and Not
Available marks (`int_wonder_county_deaths`); the 2024 rows repeated identically in the shorter single-race export are
held. `dbt/seeds/gold/county_health_measures_bridge.csv` maps the 56 registry controls of these sources.

CMS Mapping Medicare Disparities prevalence (`int_mmd_prevalence`) has one row per published file row (condition label, year
and county or state) for its one filter set. Each row keeps the published `condition_label`, the optional `file_control` (the control the file name
carries) and its source lineage. Each row takes its unit from the reviewed units seed (`dbt/seeds/mmd_condition_units.csv`). A
silver row carries no registry control: the gold MMD bridge (`dbt/seeds/gold/mmd_conditions_bridge.csv`) maps each label to
its control. `scripts/lakehouse/mmd_conditions.py` writes the units seed from the pinned collection plans and checks that the
bridge maps every plan label to its control exactly once, so the earlier acute myocardial infarction captures are mapped too. Zeros keep a possible-suppression flag and county rows without a name are flagged as unknown counties.
C258.78 is a state rate per 100,000. All 82 MMD controls stay held for definition. HRSA primary-care HPSA components
(`int_hpsa_components`) and MUA and MUP components (`int_mua_components`) keep each designation's type, status, score and
dates as published at the capture, with `XXXXX` county codes kept as tokens; rows repeated identically in the file are held.
An HPSA tract, subdivision or county ID gives the county a token hides. A facility's postal code gives one through HUD
when that ZIP lies in one county only (`county_source`).
The period seed dates each MMD file by the year in its name and the HPSA and MUA files by their capture date, which the
models carry as `capture_date`. The Federal Register HPSA workbooks stay in bronze. `dbt/seeds/gold/shortage_measures_bridge.csv`
maps the HPSA and MUA controls (C249 to C251) and the MMD bridge the 82 MMD controls, 85 registry controls in all
(rules in `dbt/seeds/features/shortage_measures_recipe.csv`).

Validation outcomes (group D) are staged as Care Compare windows with the same latest-release rule: unplanned hospital
visits (`int_cc_unplanned_visits_windows`), complications and deaths (`int_cc_complications_deaths_windows`) and the
Hospital Readmissions Reduction Program by condition (`int_cc_hrrp_windows`); their holds are in
`int_validation_window_holds`, apart from `int_cc_window_holds`. Every published measure ID is kept;
the gold bridge `dbt/seeds/gold/validation_measures_bridge.csv` maps the exact IDs the registry names (C289 to C291 and
their children). A renamed ID enters under its exact ID only through the reviewed aliases in `dbt/seeds/measure_id_aliases.csv`
(`OP-32`) and only where the exact ID has no row for that hospital and window; `published_measure_id` keeps the published
ID. The PSI-90 and PSI-13 controls (E038, E039) read the
published IDs `PSI_90` and `PSI_13` (registry revision 3); the older `PSI_90_SAFETY` and `PSI_13_POST_SEPSIS` are not
aliased (`int_cc_complications_deaths_windows`). `Not Available`, `Not Applicable`, `N/A` and `Too Few to Report` stay text. In every
group D model a 5-digit hospital ID is padded to 6 characters; any other ID that is not 6 digits or capital letters is
held. These tables are for validation only; none is a predictor.

The HAC Reduction Program (`int_hac_program_years`) and Hospital VBP Total Performance Score (`int_vbp_program_years`)
are staged per hospital and program fiscal year from the latest release, so a revised file replaces the original. The
HAC model keeps the payment reduction, Total HAC Score, PSI-90 and domain z-scores; the HAI SIRs that file republishes
are not staged, so the outcome comes only from the HAI windows. The VBP model keeps every published domain score as
published; clinical care (older files) and clinical outcomes (later files) stay separate columns. Three older VBP files
publish no fiscal year and take a reviewed year from `dbt/seeds/vbp_file_fiscal_years.csv`, whose evidence is the
published national average score. Rows the program-year models leave out are held in `int_validation_program_holds` with a reason (no_key,
no_fiscal_year, label_held, undated_release, same_date_conflict or repeated_in_file). The gold bridge
`dbt/seeds/gold/validation_measures_bridge.csv` maps the registry controls (C285 to C288); all are
comparison_only or exclude_primary.

## Coverage and Known Limits

Staging covers the HAI, cost report, IPPS, occupational-mix, Provider of Services, Hospital General Information,
ownership, Medicare inpatient, HHS capacity, ONC, Care Compare timely and effective care, maternal health and HCAHPS
tables, the geography tables (HUD ZIP-to-county, county adjacency, RUCC, RUCA and hospital service areas), the ACS and
SVI tables, SAIPE, SAHIE, BLS, PLACES, Medicare Geographic Variation, WONDER, MMD and the HRSA HPSA and MUA files. It
types the HAI and Care Compare windows, the geography files, the ACS, SVI, SAIPE, SAHIE, BLS, PLACES, geographic
variation, WONDER and MMD values, the HPSA and MUA designations, the group D validation windows (unplanned visits,
complications and deaths, readmissions reduction) and HAC Reduction and VBP program years, the occupational-mix
surveys, the cost reports, the IPPS impact-file fields, the Medicare inpatient summaries, the owner, enrollment and
change-of-ownership releases, the HHS capacity weeks, the ONC linkage and attestations, the Provider of Services
hospital snapshots, Hospital General Information and the case-mix index. The Medicare sepsis share counts only
published DRG cells (11 discharges or more), so it can be low for small hospitals; the Medicare chronic-condition
shares start with data year 2017. Each change-of-ownership release lists events back to 2016; silver keeps each event
once from its latest release and holds the other releases' rows that differ or repeat
(`int_change_of_ownership_holds`). An enrollment or change-of-ownership value with a location suffix, a lost leading
zero or a unit letter maps to its parent CCN when POS lists exactly one such parent in the row's state (`ccn_source`);
any other value keeps no CCN. An HHS facility without a CCN gets one only from the reviewed matches in
`dbt/seeds/hhs_reviewed_ccn_matches.csv`. Registry revision 3 closes the HHS part of controls C001 and C040, so the
HHS columns are staged but not mapped to them. The derived values and registry mappings that silver used to carry (for
example the occupational-mix shares and the MMD and ACS control maps) have left silver: the formulas are saved in
`features/` and the control maps are gold bridge seeds.
