---
title: Saved for the Feature Phase
description: The silver models saved in features/ for the feature phase, as described when they were silver models.
last_updated: 2026-10-15
---

# Saved for the Feature Phase

These models are saved copies in `features/` (`features/models/`, `features/tests/` and `features/macros/`) that dbt does
not build. Their descriptions stay here as written when they were silver models.

The case-mix index models choose one CMI per hospital and rule year from the year's best rule stage; disagreeing files are held in
`int_cmi_holds`. `int_hospital_spine` has one row per hospital and calendar-year HAI window, as of the window start: the
Provider of Services snapshot that ends in the 12 months before the window and the CMI of the fiscal year that ends before
it (`int_cmi_hospital_data_years`), with the primary and sensitivity population flags. A window missing its own snapshot
takes its state and county from the same hospital's other snapshots within 24 months, then the enrollment file, then the
CCN's state code. A county can also come from the hospital ZIP when the HUD crosswalk places that ZIP in one county only; a
ZIP split across counties gives none. Classification is filled only when the neighboring snapshots agree. Each filled field
names its source. Connecticut windows also carry their planning region when the ZIP lies in one region.

These saved models still read the retired registry-map seeds of commit `e918ed7`; the same mappings now live in the gold
control bridges and recipes named below, which a feature-phase rewrite reads instead.
`int_registry_measure_windows` carried each registry measure of the Care Compare timely and effective care, maternal health and
HCAHPS families by the exact measure ID in `dbt/seeds/registry_measure_sources.csv` (now
`dbt/seeds/gold/registry_measure_sources_bridge.csv`).
`int_cost_report_measures` computed each registry cost-report measure by the
columns and rule in `dbt/seeds/cost_report_measures.csv` (now `dbt/seeds/gold/cost_report_measures_bridge.csv` and
`features/recipes/cost_report_measures_recipe.csv`) and gave null for a zero or missing denominator. `int_impact_measures` carried the registry measures of source CMS_IPPS (`dbt/seeds/impact_measures.csv`, now
`dbt/seeds/gold/impact_measures_bridge.csv`). `int_mup_measures` computed the registry
measures of sources CMS-MUP-PROVIDER and CMS_MEDICARE_PROVIDER by the fields and rule in `dbt/seeds/mup_measures.csv` (now
`dbt/seeds/gold/mup_measures_bridge.csv` and `features/recipes/mup_measures_recipe.csv`). `int_validation_program_values` carried the registry controls (C285 to C288); all
are comparison_only or exclude_primary. `int_validation_measure_windows` carried the PSI-90 and PSI-13 controls (E038, E039): they read the
published IDs `PSI_90` and `PSI_13` (registry revision 3); the older `PSI_90_SAFETY` and `PSI_13_POST_SEPSIS` are not
aliased.

The alignment stage lines each staged source up with the hospital-year spine as of the HAI window start. It was built
in silver at commit `e71779d` and moves to the feature phase under the October 10 2026 layer design (see the [architecture diagram](architecture/architecture.png)).
`int_spine_hai_outcomes` (step AL1) has one row per spine hospital-window and infection type (HAI_1 CLABSI to HAI_6
C. difficile): the published SIR, its bounds, the observed and predicted counts and the exposure, by exact measure ID,
with tokens kept as text and the SIR footnote codes. No SIR is computed from the counts. A part from a release after the
last 2015-baseline review (August 13 2026) is held and counted, so 2022-baseline SIRs never join the series; a part
held in staging is counted too. Each row has an alignment status (aligned, held_in_staging, not_in_source), every part's
footnote and the SIR's national benchmark category, which `int_hai_hospital_windows` keeps as published. Each type
stays separate; the model target is chosen before training. `int_hai_outcome_values` carries the registry's HAI controls
(C269 to C283 and C283's children) by the part `dbt/seeds/hai_outcome_measures.csv` (now `dbt/seeds/gold/hai_outcome_measures_bridge.csv`) names: C269 to C274 as target
candidates and C275 to C280, the same SIRs, as earlier-outcome forecasting candidates.
`int_spine_care_compare_measures` (step AL2) has one row per spine hospital-window and Care Compare process or structure
control in `dbt/seeds/registry_measure_sources.csv` (now `dbt/seeds/gold/registry_measure_sources_bridge.csv`) (families S13 to S16). Each control takes the latest measure window
that ends before the HAI window starts; the overall rating (C284, excluded from the primary predictors) takes the latest
Hospital General Information release dated before the start, held when two files on that date disagree. Values stay as
published with their age in months and no age limit. C141's spellings (for example `low` and `Low`) each map to one
category. A test fails on any new spelling. A control that starts after a window (for example C161 to C173 from
2024) is kept with the status `no_period_before_start`; the other statuses are aligned, held_in_staging and
not_in_source.
`int_spine_hospital_measures` (step AL3a) does the same for the hospital finance and operations controls, by source and
field: the latest cost report whose period ends before the start (several reports ending that day are compared), the
IPPS impact value of the latest rule fiscal year that ends before the start (October to September, the most final stage
first), the latest Medicare inpatient data year and the latest occupational-mix survey. Values that disagree for one
period are held and counted. Occupational-mix copies of one survey (text and workbook twins in several rule-year files)
within 0.0001 of each other count as one value.
`int_spine_operations_measures` (step AL3b) adds the operations controls as each registry field defines them: HHS capacity
from the weeks of the calendar year before the window (a ratio of sums for two fields, a sum of 7-day sums or a mean of
7-day averages; a week where two HHS hospitals share a CCN is skipped and counted), ONC certified-EHR status, developers
and product count from the latest performance period, private-equity and REIT ownership from the latest owner release
before the start (direct, indirect and partnership owners only; a blank is `not_reported`, never no). Changes of
ownership count those in the year before the start, with the completed months since the latest one.
`int_spine_validation_measures` (step AL5) places the validation outcomes (HAC and VBP program years, HRRP, unplanned
visits and 30-day mortality) against each HAI window's own period, because they are compared with the window, never used
to predict it: the published window equal to the calendar year first, otherwise the one with the largest overlap. HAC
and VBP years are placed by the HAI measure period HAC publishes for each fiscal year. No model reads this table; an E2E
check fails if one does.
`int_spine_county_measures` (step AL4a) adds the county context from PLACES, Medicare Geographic Variation, CDC WONDER,
Mapping Medicare Disparities and RUCC through the spine's county: the latest data year (RUCC vintage) that ends
before the window starts. PLACES counts only all-state measure-years and takes the latest release of each data year;
WONDER 2018 to 2020 come from the single-race database. Connecticut matches a source on its old county or its planning
region.
`int_spine_county_context` (step AL4b) adds ACS, SVI, SAIPE, SAHIE (all-group rows) and BLS (annual averages) by the same
rule, HPSA and MUA designations in force at the window start (rebuilt from their designation and withdrawal dates, with
the score as published at the capture; an undated withdrawal holds the window) and RUCA codes for the hospital ZIP.
`int_spine_linkage` keeps the HUD residential share of the hospital ZIP and the hospital's service-area ZIPs and cases
apart as linkage, never predictors.

History is type 2 (SCD2), built in silver (under the October 10 2026 layer design the history method moves to the
Feast phase and gold keeps the snapshot and release rows) and dated by the publishers' own dates, so a clean checkout rebuilds the
same versions: `int_hospital_pos_history` (Provider of Services attributes per snapshot), `int_hospital_hgi_history`
(Hospital General Information descriptions per release; the overall rating stays a measure) and
`int_hospital_ownership_history` (organization owners only, by enrollment, owner and role, per release). The macro
`features/macros/scd2.sql` starts a version when a tracked value changes or when a hospital or owner returns after a release
without it; that release closes the earlier version. A release that lists one key twice with different values is held
for that release. Some CMS owner and enrollment releases drop the leading zeros of the 10-digit associate IDs, so the
row models pad them back and mark the row (`associate_ids_padded`). Ownership history bridges an owner missing from one
release and counts it in `bridged_releases` (owner decision, October 10 2026). A held release is never bridged; POS and
Hospital General Information history do not bridge. `features/tests/assert_history_versions_valid.sql` fails on overlapping windows or a
second current version.
