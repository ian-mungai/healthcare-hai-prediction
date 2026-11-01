---
title: Staging models
description: What each staging and intermediate model keeps, holds and maps, and the coverage limits of the staging layer.
last_updated: 2026-10-29
---

# Staging models

This document describes what each staging and intermediate dbt model keeps, holds and maps. It also states where the staging layer stops. It is for developers who change a staging model or a seed.

## Build and checks

The dbt staging layer (`dbt/`, dbt-core with dbt-duckdb in the analytics image) reads bronze read-only. The staging E2E runs in three parts:

1. It builds the models on synthetic fixtures, including cases that must fail one named test.
2. It builds the models on the real bronze tables twice.
3. It reconciles each staging table with bronze.

Before each real build, the E2E pins the current snapshot of every mapped bronze table in a new run folder. After the build it verifies that no table moved (`scripts/lakehouse/pins.py`, run in the Spark container as `catalog job pins`). A moved table fails the run.

Where a text file and a workbook hold the same IPPS or occupational-mix table, staging reads the text file. It drops only the workbook sheets that a selected text file of the same release matches row for row. Every other sheet stays selected (`stg_bronze__sheet_selection`). `scripts.lakehouse.ipps_file_labels` regenerates the IPPS and occupational-mix label and twin seeds from the S3 manifests. `--check` confirms the committed seeds.

## County changes and Provider of Services

`int_county_retirements` has one row per county code that a Census county-change page (1970, 1980, 1990 or 2000) retires, with the latest retirement and its successors. It reads the seed `census_county_change_events`. `scripts.lakehouse.county_change_seed` writes that seed from the four committed, pinned sections of the pages, using the one reader `scripts/acquisition/county_change_pages.py`. Staging never reads the pages or the sections. `county_change_seed --check` regenerates the seed from the sections and compares the events with the golden file. A code that a later page creates or renames to again is not retired (02230). `dbt/seeds/alaska_county_code_labels.csv` adds the reviewed historical name of the six Alaska codes beside the unchanged published code. Gold `dim_county` passes the retirement columns through. A code without a row there has nulls, which is not a statement that the code is current.

`int_hospital_owner_periods` has one row per hospital CCN, organisation owner associate ID and role code per continuous period of the monthly owner releases. It reads `int_hospital_owner_rows` and `int_hospital_enrollment_rows`. A period starts at the first release that lists the owner in the role and ends at the release after the last one. One missing release is bridged and counted in `bridged_releases`. History is recomputed from every release on each build, so a corrected release can change or remove a period. It holds no name, contact or share. Gold `dim_hospital_owner_history` keys it.

`int_pos_hospital_snapshots` types the Provider of Services hospital rows. The catalog coverage in `dbt/seeds/pos_file_periods.csv` dates each file. `scripts.lakehouse.pos_file_periods` rebuilds that seed from the S3 manifests and the local acquisition job plans. The model fills the county of a snapshot row in three steps:

1. A valid published county is kept (`county_source` pos).
2. A county that is missing or outside the state of the snapshot takes the county of the ZIP of the snapshot. This applies when the HUD quarter in force on the period end of the snapshot lists that ZIP, with a residential ratio above 0, in exactly one county under the state of the snapshot (`county_source` hud_zip_crosswalk). Connecticut fills only from 090xx county codes and keeps a published 091xx county.
3. Otherwise the county stays null and `county_fill_reason` says why.

`county_fips_published` keeps the published value.

## Case-mix index, Care Compare windows and cost reports

- The case-mix index models read the unadjusted CMI by exact header name (`dbt/seeds/cmi_layout_columns.csv`). They keep the transfer-adjusted CMI in its own column, with one row per CCN in each selected file and each rule year and stage (`int_cmi_hospital_rows`). The feature phase chooses one CMI per hospital and rule year (see [Saved for the feature phase](feature_phase_saved.md)).
- The Care Compare timely and effective care, maternal health and HCAHPS tables get the same window models as HAI (`int_cc_*_windows`, with holds in `int_cc_window_holds`).
- `int_hgi_hospital_releases` types Hospital General Information per file.
- `int_cost_reports` has one row per hospital cost report, in the federal fiscal year in which its period starts. A column gives the number of reports of its CCN in that year.
- The IPPS impact files from FY 2001 are read by exact header name (`dbt/seeds/impact_layout_columns.csv`). `int_impact_hospital_values` has one row per release, hospital and field.
- The Medicare inpatient provider and DRG summaries are typed per file, with the data and release years from the file name (`int_mup_providers`, `int_mup_drg_discharges`). A blank, suppressed cell is null.

## Registry control bridges and recipes

The registry control mappings are the gold control bridge seeds `dbt/seeds/gold/<name>_bridge.csv`. The registry-map seeds in `dbt/seeds/` beside the staging seeds are retired. Each bridge keeps the stem of its retired seed plus `_bridge`. `scripts/lakehouse/bridge_references.py --check` checks the bridges against `data_contracts/lakehouse/gold_contract.json`.

The formula rules are in `dbt/seeds/features/<name>_recipe.csv`. Recipes exist for the cost report, IPPS impact, Medicare inpatient provider, occupational-mix, HHS and ONC, MMD and shortage families. The other families have no recipe.

These bridges map the registry controls of the models above:

- `registry_measure_sources_bridge.csv` maps the Care Compare measures of the window models by exact measure ID.
- `hai_outcome_measures_bridge.csv` maps the HAI controls of the registry (C269 to C283 and the children of C283) to the published HAI parts.
- `cost_report_measures_bridge.csv` maps the cost-report measures.
- `impact_measures_bridge.csv` maps the registry measures of source CMS_IPPS.
- `mup_measures_bridge.csv` maps the registry measures of sources CMS-MUP-PROVIDER and CMS_MEDICARE_PROVIDER.

Silver no longer carries the derived values and registry mappings that it once carried (for example the occupational-mix shares and the MMD and ACS control maps). The formulas are saved in `features/`. The control maps are gold bridge seeds.

## Ownership

- The owner, enrollment and change-of-ownership files are typed per release (`int_hospital_owner_rows`, `int_hospital_enrollment_rows`, `int_change_of_ownership_rows`).
- The publisher's catalog period in `dbt/seeds/ownership_release_periods.csv` dates each file. `scripts.lakehouse.ownership_release_periods` rebuilds that seed from the S3 manifests and the local capture receipts.
- A change of ownership listed in several releases is kept once, from the latest release. The latest catalog period end decides first, then the highest release ID, then the lower source row and checksum.
- Another release whose published values differ is held in `int_change_of_ownership_holds`. So is a repeated row of the event in the kept release. Values are compared as published, so associate IDs 123 and 0123 differ.
- The staging models pad associate IDs that lost leading zeros to 10 digits and mark the row.
- An owner flag that the layout of a release lacks (private equity and REIT before April 2025) is null. `dbt/seeds/gold/ownership_measures_bridge.csv` names the columns each registry control uses.

## HHS capacity and ONC

- The HHS weekly hospital capacity rows are typed per hospital and week (`int_hhs_capacity_weeks`). A count of 1 to 3, published as -999999, is null and listed in `suppressed_fields`. Any other negative value is null and listed in `negative_fields`.
- The ONC Promoting Interoperability to certified-product linkage and the older 2011 to 2017 EHR incentive attestations are typed apart (`int_onc_chpl_linkage_rows`, `int_onc_attestation_rows`).
- `dbt/seeds/gold/hhs_onc_measures_bridge.csv` maps the registry controls. The formula rules are in `dbt/seeds/features/hhs_onc_measures_recipe.csv`. The stored `columns.json` resolves the HHS field names that the source truncates.

## Occupational-mix surveys

The survey rows are typed per file, sheet and source row (`int_occmix_survey_rows`).

- Exact headers distinguish survey records from CBSA and S-3 tables. The layout inventory retains every selected file and sheet.
- Survey dates come from FROM and TO, including workbook ISO dates. Payment-rule years stay separate.
- Deleted records, invalid identifiers or periods and duplicate provider-period rows remain visible with a hold and no derived values.
- `dbt/seeds/gold/occmix_measures_bridge.csv` maps all six S03 registry controls (rules in `dbt/seeds/features/occmix_measures_recipe.csv`). They are RN paid hours, the RN, combined LPN/LVN plus surgical-technologist and NAORAT paid-hour shares and RN salary per paid hour. Missing values and nonpositive denominators give null.
- C043 stays unavailable. Paid hours cannot substitute for direct-care worked hours per patient-day. Registry revision 3 closes it as unavailable from public sources.
- Comma-grouped numbers are parsed only when their grouping is valid. Dots, dashes and blanks remain null.
- Source revisions remain separate. Hospital-window alignment, publication timing and model eligibility are pending.

## Geography

The geography sources are typed at their own grain.

- HUD ZIP-to-county rows carry the USPS quarter that their capture receipt records (`int_hud_zip_county_quarters`), with each address ratio kept separately. The rows whose code is not a county are held apart (`int_hud_zip_county_holds`). The last real build held 47 of them. Count them with `select count(*) from int_hud_zip_county_holds` against the `build.duckdb` of the run.
- County adjacency covers the 2010 and 2023 to 2026 files, with self-links and islands flagged (`int_county_adjacency_edges`).
- Rural-Urban Continuum Codes (2013, 2023) and Rural-Urban Commuting Area codes (2010, 2020, for tracts and ZIP codes) stay text categories (`int_rucc_county_codes`, `int_ruca_codes`).
- Hospital service area counts keep their suppression marks as flags (`int_hsa_zip_cases`).
- County codes are 5-character text. Connecticut is flagged and not removed.
- `scripts/lakehouse/geography_file_periods.py` generates `dbt/seeds/geography_file_periods.csv`, which dates every file. HUD and service-area files take their dates from their acquisition records. RUCC, RUCA and adjacency files take a reviewed vintage that is not a publication date.
- `dbt/seeds/gold/geography_measures_bridge.csv` maps the eight registry controls of these families. Weights, graph rules and hospital-window alignment are pending.

## ACS and SVI

- ACS 5-year values for 2010 to 2024 come from the data.census.gov exports and, for the detailed tables from 2018, the summary files (`int_acs_county_values`, one row per county, vintage and concept).
- ACS column codes change meaning between vintages. `dbt/seeds/acs_variable_map.csv` therefore names, per concept and vintage, the column and the exact label it must carry. A test fails when the label of a file differs.
- A change in the wording of a meaning holds the vintage instead of mixing it in. So do the subject-table units that changed from percents to counts in 2017.
- Missing marks and sentinels stay as tokens. A capped median keeps its cap and a flag.
- The seven SVI editions are staged long under the own field names of each edition (`int_svi_county_values`), with -999 kept as a token.
- `dbt/seeds/geography_file_periods.csv` gives the ACS file vintage and the SVI edition.
- `dbt/seeds/gold/acs_svi_measures_bridge.csv` maps 43 of the 44 S18 and S19 controls to ACS concepts and SVI fields. `dbt/seeds/gold/income_labor_measures_bridge.csv` holds C177.02. Staging picks no source between them.

## SAIPE, SAHIE and BLS

- SAIPE county estimates for 1989 to 2024 are cut from the fixed-width files at the positions that the layout of every year documents (`int_saipe_county_estimates`). They hold poverty counts and percents with 90% bounds and median household income in nominal dollars.
- SAHIE county rows for 2006 to 2024 keep their published age, race, sex and income groups (`int_sahie_county_rows`). A test checks that the preamble of each file defines code 0 as under 65, all races, both sexes and all incomes. That group is the one the registry names.
- BLS LAUS county series (`int_bls_county_series`) keep months and annual averages apart, with footnote codes and the capture date as the revision vintage. `.` and `-` stay as missing marks.
- `dbt/seeds/gold/income_labor_measures_bridge.csv` maps the SAIPE, SAHIE and BLS registry controls.

## PLACES, geographic variation and WONDER

- PLACES county values keep their release, data year, measure and crude or age-adjusted type (`int_places_county_values`). `is_all_states` marks the measure-years whose county rows cover all 50 states and DC. The project uses only those. The 2020 release has no county codes. Its rows take the code that PLACES publishes for the same state and county name in its other releases (`county_source`).
- Medicare Geographic Variation county values are staged long for every published age level, with `*` and `NA` kept as tokens (`int_gv_county_values`).
- WONDER deaths and crude rates keep their database, bridged race (1999 to 2020) or single race (2018 to 2024), with their Suppressed, Unreliable, Missing and Not Available marks (`int_wonder_county_deaths`). The model holds the 2024 rows that the shorter single-race export repeats identically.
- `dbt/seeds/gold/county_health_measures_bridge.csv` maps the 56 registry controls of these sources.

## Mapping Medicare Disparities and shortage areas

- CMS Mapping Medicare Disparities prevalence (`int_mmd_prevalence`) has one row per published file row (condition label, year and county or state) for its one filter set. Each row keeps the published `condition_label`, the optional `file_control` (the control the file name carries) and its source lineage. Each row takes its unit from the reviewed units seed (`dbt/seeds/mmd_condition_units.csv`).
- A silver row carries no registry control. The gold MMD bridge (`dbt/seeds/gold/mmd_conditions_bridge.csv`) maps each label to its control. `scripts/lakehouse/mmd_conditions.py` writes the units seed from the pinned collection plans. It checks that the bridge maps every plan label to its control exactly once, so the earlier acute myocardial infarction captures are mapped too.
- Zeros keep a possible-suppression flag. County rows without a name are flagged as unknown counties.
- C258.78 is a state rate per 100,000. All 82 MMD controls stay held for definition.
- HRSA primary-care HPSA components (`int_hpsa_components`) and MUA and MUP components (`int_mua_components`) keep the type, status, score and dates of each designation as published at the capture. They keep `XXXXX` county codes as tokens. The models hold rows that the file repeats identically.
- An HPSA tract, subdivision or county ID gives the county that a token hides. The postal code of a facility gives it through HUD when that ZIP lies in one county only (`county_source`).
- The period seed dates each MMD file by the year in its name and the HPSA and MUA files by their capture date, which the models carry as `capture_date`. The Federal Register HPSA workbooks stay in bronze.
- `dbt/seeds/gold/shortage_measures_bridge.csv` maps the HPSA and MUA controls (C249 to C251). The MMD bridge maps the 82 MMD controls. Together they map 85 registry controls (rules in `dbt/seeds/features/shortage_measures_recipe.csv`).

## Validation outcomes

Validation outcomes are staged as Care Compare windows with the same latest-release rule. The windows are unplanned hospital visits (`int_cc_unplanned_visits_windows`), complications and deaths (`int_cc_complications_deaths_windows`) and the Hospital Readmissions Reduction Program by condition (`int_cc_hrrp_windows`). Their holds are in `int_validation_window_holds`, apart from `int_cc_window_holds`.

- Every published measure ID is kept. The gold bridge `dbt/seeds/gold/validation_measures_bridge.csv` maps the exact IDs the registry names (C289 to C291 and their children).
- A renamed ID enters under its exact ID only through the reviewed aliases in `dbt/seeds/measure_id_aliases.csv` (`OP-32`). It enters only where the exact ID has no row for that hospital and window. `published_measure_id` keeps the published ID.
- The PSI-90 and PSI-13 controls (E038, E039) read the published IDs `PSI_90` and `PSI_13` (registry revision 3). The older IDs `PSI_90_SAFETY` and `PSI_13_POST_SEPSIS` have no alias (`int_cc_complications_deaths_windows`).
- `Not Available`, `Not Applicable`, `N/A` and `Too Few to Report` stay text.
- In every validation model, a 5-digit hospital ID is padded to 6 characters. The model holds any other ID that is not 6 digits or capital letters.
- These tables serve validation only. None is a predictor.

## HAC Reduction and VBP program years

The HAC Reduction Program (`int_hac_program_years`) and Hospital VBP Total Performance Score (`int_vbp_program_years`) are staged per hospital and program fiscal year from the latest release, so a revised file replaces the original.

- The HAC model keeps the payment reduction, Total HAC Score, PSI-90 and domain z-scores. It does not stage the HAI SIRs that the file republishes, so the outcome comes only from the HAI windows.
- The VBP model keeps every published domain score as published. Clinical care (older files) and clinical outcomes (later files) stay separate columns.
- Three older VBP files publish no fiscal year. They take a reviewed year from `dbt/seeds/vbp_file_fiscal_years.csv`, whose evidence is the published national average score.
- The models hold the rows they leave out in `int_validation_program_holds` with a reason (no_key, no_fiscal_year, label_held, undated_release, same_date_conflict or repeated_in_file).
- The gold bridge `dbt/seeds/gold/validation_measures_bridge.csv` maps the registry controls C285 to C288. All of them are comparison_only or exclude_primary.

## Coverage and known limits

Staging covers these tables:

- the HAI, cost report, IPPS, occupational-mix, Provider of Services, Hospital General Information, ownership, Medicare inpatient, HHS capacity, ONC, Care Compare timely and effective care, maternal health and HCAHPS tables
- the geography tables (HUD ZIP-to-county, county adjacency, RUCC, RUCA and hospital service areas)
- the ACS and SVI tables, SAIPE, SAHIE, BLS, PLACES, Medicare Geographic Variation, WONDER, MMD and the HRSA HPSA and MUA files

Staging types these values:

- the HAI and Care Compare windows
- the geography files
- the ACS, SVI, SAIPE, SAHIE, BLS, PLACES, geographic variation, WONDER and MMD values
- the HPSA and MUA designations
- the validation windows (unplanned visits, complications and deaths, readmissions reduction)
- the HAC Reduction and VBP program years
- the occupational-mix surveys, the cost reports and the IPPS impact-file fields
- the Medicare inpatient summaries
- the owner, enrollment and change-of-ownership releases
- the HHS capacity weeks, the ONC linkage and attestations
- the Provider of Services hospital snapshots, Hospital General Information and the case-mix index

These limits apply:

- The Medicare sepsis share counts only published DRG cells (11 discharges or more), so it can be low for small hospitals.
- The Medicare chronic-condition shares start with data year 2017.
- Each change-of-ownership release lists events back to 2016. Silver keeps each event once from its latest release. It holds the rows of the other releases that differ or repeat (`int_change_of_ownership_holds`).
- An enrollment or change-of-ownership value with a location suffix, a lost leading zero or a unit letter maps to its parent CCN when POS lists exactly one such parent in the state of the row (`ccn_source`). Any other value keeps no CCN.
- An HHS facility without a CCN gets one only from the reviewed matches in `dbt/seeds/hhs_reviewed_ccn_matches.csv`.
- Registry revision 3 closes the HHS part of controls C001 and C040, so the HHS columns are staged but not mapped to them.
