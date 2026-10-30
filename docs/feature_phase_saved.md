---
title: Saved for the feature phase
description: The feature models in dbt/models/features, the checks that limit what they read and the rule of each model.
last_updated: 2026-10-29
---

# Saved for the feature phase

This document describes the feature phase: the models that were silver models before the Oct 10 2026 layer design, the way dbt builds them and the checks that limit what they read. It is for developers who change a feature model, a feature macro or one of these checks.

## Where the feature phase lives

- The models are in `dbt/models/features/`, their macros in `dbt/macros/features/` and their singular tests in `dbt/tests/features/`.
- dbt builds them as views after gold, never inside the gold build. They read gold facts, dimensions and bridges instead of silver.
- The dbt tag `features` marks them. The gold build selections exclude that tag. A command with no selection uses the default selector in `dbt/selectors.yml`, which leaves them out. Select them with `--select tag:features`.
- The spine and the case-mix index models are tables in the build's DuckDB file only.
- The formula recipes are the seeds in `dbt/seeds/features/`. They carry the tag `features` too, so the gold selections leave them out and the feature build loads them.
- `features/` keeps the archived Great Expectations suites and `archive_manifest.json`.

## Source check

`scripts/lakehouse/feature_sources.py` fails when a feature model, macro or test reads a silver model, a retired seed or bronze. It runs two passes.

The text pass is fast. It reads the references in the text (a quoted `ref()` or `source()`, a silver relation named in SQL text). It rejects any reference it cannot resolve there, such as one built from a Jinja variable.

The resolved pass does not depend on the spelling of a read. It has two layers. The second starts only when the first finds nothing:

1. The admission layer (see "Admission lists" below) runs on a parse and builds nothing. Any problem it reports ends the check before a dbt build, compile, seed or query starts.
2. The build layer builds the feature phase in two stages (`feature_columns.run_features`):
   - Stage 1 is no dbt run. The check builds the 15 filter tables itself in DuckDB from a database that holds the gold-contract tables, the raw bridge included (`create table <filter> as select * from bridge_measure_control where source_seed = ?`, with the seed taken from the file name). No hook, macro or configuration of the project can change their rows.
   - Stage 2 runs every other feature model with dbt on a DuckDB file that holds only the relations a feature model may read, with external access off. These relations are the published gold tables of the frozen contract without `bridge_measure_control`, the seeds in `dbt/seeds/features`, the live reference seeds directly under `dbt/seeds` and the filter tables. The scratch copy of the project runs with `on-run-start` and `on-run-end` emptied.

The resolved pass reads the selection of `dbt ls --select tag:features`. It binds the compiled query of every selected model and test there, each on a database that holds only the relations pinned for that node. The generic tests that a YAML file in a feature folder declares are included. So are the seed tests. The pass fails by name on:

- a selected node of any other type (snapshot, analysis, unit test, source)
- a macro that overrides `ref`, `source`, a materialization or a macro of dbt or dbt-duckdb
- a filter model whose compiled query is not the template
- a top-level key of `dbt/dbt_project.yml` outside `feature_sources.PROJECT_KEYS` (`dispatch` and `query-comment` among them)
- a run hook other than the two pinned build marker hooks (`feature_sources.RUN_HOOKS`)
- a relation in the stage-2 database after the run and the compile that neither the restricted database nor a feature model gives

The pass also fails on a relation whose rows (count and digest) differ from before the run. A read of silver, bronze, a retired seed, a bridge seed, the raw bridge or any other relation fails because the relation does not exist. This holds whether the name is written in upper case, quoted, aliased, counted, built with `query_table`, `var()`, a macro or an adapter relation. A table function that reads a file or another database fails too. The pass also reads the dependencies in the parsed dbt manifest.

## Admission lists

SQL can run outside the compiled query of a node. Examples are a `run_query()` that rewrites a filter table or creates, reads and drops the raw bridge, a `statement` block, `adapter.execute` and a `sql_header`. Such SQL leaves no trace in the compiled query, so the check cannot look for it. The check admits only what a closed list contains (`scripts/lakehouse/feature_allowlist.py`). It runs before anything builds, on a parse of a scratch copy of the project. The scope is every selected node, every seed and the two gold models that the shell build runs. The lists are:

- **Jinja** of each node and of each project macro it reaches (transitively, from `depends_on.macros`), parsed with dbt's own Jinja. Only the constructs of `NODE_TYPES` are allowed: text, output, `set` of a name, `if`, `for`, `not`, comparison, `~`, a conditional expression, list, tuple, `join`, `loop.first`, `loop.last` and macro blocks. Calls are allowed only to `ref`, `config` (with `alias`, `column_types`, `contract`, `materialized`, `meta` or `tags` only), `return` and the macros the code depends on. Every name the code reads without declaring it must be one of those. So `run_query`, `statement`, call blocks, `do`, `adapter`, `execute`, `target`, `var`, `env_var`, `modules`, `log` and dict literals fail with the node, file and line. Nothing environment-dependent can reach a `ref`, so a `ref` resolves the same at parse and at run time.
- **dbt macros** a node may reach, by exact ID (`PACKAGE_MACROS`): the generic tests `unique`, `not_null` and `accepted_values` with their default implementations, plus the `get_where_subquery` and `should_store_failures` macros that they call. Any other macro of dbt, dbt-duckdb or a package fails. A project macro with the name of a dbt context function (`config`, `var`, `return`, `log`, `local_md5` and the rest of dbt's context) fails, because it would replace that function for every node.
- **Config** resolved by dbt (node, folder and project levels merged). Only the keys of `INERT` with dbt's default value are allowed (for example `enabled`, `grants`, `persist_docs`, `on_schema_change`, `where` and `severity`). The keys of `FREE` are allowed with the values their rule accepts (`materialized` view or table, `tags`, `meta`, `alias`, `contract`, `column_types`). A `sql_header`, `persist_docs`, `grants` or any other key fails. The SQL text that a column `data_type` or a seed `column_types` entry carries into a create-table statement must be a plain type text. A constraint fails.
- **YAML** property files of the project. A Jinja string in a generic test argument or a description may call `ref` only (dbt renders it at parse time).
- **Run hooks.** The SHA-256 of the code of each hook macro and of each project macro they reach is pinned (`HOOK_MACRO_SHA256`). A change fails the check and names the macro. The scratch builds run with both hook lists emptied.
- **Hooks.** A `pre-hook` or `post-hook` on a node in scope or on any seed fails at admission. So does a run hook that a package adds.
- **Inputs.** `dbt/models/features/_feature_inputs.json` lists, for each feature model and singular test, the nodes it may `ref()` (gold facts, seeds, other feature models and the filter tables of its own sources). A `ref()` outside the list, a model or test without an entry and an entry without a node fail at admission. A generic test may read the node it tests and nothing else. After the build, each model and test is bound on a database that holds only its pinned relations. A filter table of another source therefore also fails when a model reads it by a relation name or `query_table`. To change an entry, edit the file by hand and have the change reviewed.

A change that needs a construct outside a list fails with the construct named. Widen the list in `feature_allowlist.py` only after review. `feature_columns --check` runs the same admission before it builds. After the build, the check compares the digest of every relation, views included. A relation that the check cannot read fails the check.

## Control bridge and column contracts

The shared control bridge `bridge_measure_control` holds the controls of all 15 retired seeds. A feature model may read it only for its own seed. The stages enforce that rule. It is not read from SQL.

The bridge filter models in `dbt/models/features/bridge` are `feature_bridge_<retired seed>.sql`, one per retired seed (15). Each is exactly the template `feature_sources.BRIDGE_FILTER` with its seed: a table `select * from bridge_measure_control where source_seed = '<seed>'`. The text pass fails on:

- a file that differs from the template
- a file in that folder that is no filter model
- a retired seed without a filter model
- a filter model that no other feature model or test reads

Every other feature model and test reads the filter model of its seed and never `bridge_measure_control`. The frozen gold contract gives no columns for some gold dimensions. A feature model that reads one of them fails until its columns are added to the contract.

Every feature model, the filter models included, has an enforced column contract (`+contract` for the folder in `dbt/dbt_project.yml`). The file `_features__models.yml` documents every output column with its data type and description. `scripts/lakehouse/feature_columns.py` compares the YAML with the columns the models give on empty gold tables, with the contract switched off. An undocumented or stale column therefore fails the check on its own. The enumerated output columns (status, type, rule, rule stage, review decision and the fields of the closed lists) have `accepted_values` tests with the values each model can produce.

The controls of the seven lookup models (Care Compare, validation, hospital, operations, HAI outcomes and the two county models) are frozen in `dbt/seeds/features/frozen_*.csv`. A script generated them once from the retired seeds at `e918ed7`. The tests `assert_*_control_identity` compare the frozen controls with the controls each model carries, with the gold control bridge and with the formula recipes. The comparison uses tuples of source, control, measure and value column (for the county lookups, `frozen_county_controls.csv`). A control swapped for another fails even when the count stays the same. So does a control redirected to another measure of the same fact.

## Case-mix index and hospital spine

The case-mix index models choose one CMI per hospital and rule year from the best rule stage of the year. They hold disagreeing files in `int_cmi_holds`.

`int_hospital_spine` has one row per hospital and calendar-year HAI window, as of the window start. Each row carries:

- the Provider of Services snapshot that ends in the 12 months before the window
- the CMI of the fiscal year that ends before the window (`int_cmi_hospital_data_years`)
- the primary and sensitivity population flags

A window missing its own snapshot takes its state and ZIP from the latest earlier snapshot of the same hospital within 24 months. If none exists, it takes them from the latest enrollment row that ends before the start. For every window, the county comes from the own or the latest earlier snapshot. Otherwise the county comes from the hospital ZIP, when the latest HUD quarter that ends before the start places that ZIP in one county only. A ZIP split across counties gives none. Classification comes from the own or the latest earlier snapshot. Each filled field names its source (`pos_own`, `pos_earlier`, `enrollment`, `hud_quarter`) and the period end of that source, which is always before the window start. Connecticut windows also carry their planning region when the ZIP lies in one region in the latest HUD quarter that ends before the start.

## Differences from the earlier silver models

The registry-map seeds of commit `e918ed7` are retired. The gold control bridges and recipes named below carry the same mappings. The feature models read them instead. The rewritten models keep their grain and output columns, except three:

- `int_spine_care_compare_measures` gives `fact` (the gold table that holds the value) instead of `family` and `source_model`.
- `int_spine_validation_measures` gives `fact` instead of `source_model`.
- `int_hai_outcome_values` has no `original_decision`. Gold does not carry the target candidates, so the owner decides whether gold or the training configuration names them.

A closed control with no measure key (the validation payment adjustment C288 and the HHS mappings C001 and C040) is not carried.

The registry models read these seeds:

- `int_registry_measure_windows` carries each registry measure of the Care Compare timely and effective care, maternal health and HCAHPS families by the exact measure ID in `dbt/seeds/registry_measure_sources.csv` (now `dbt/seeds/gold/registry_measure_sources_bridge.csv`).
- `int_cost_report_measures` computes each registry cost-report measure by the columns and rule in `dbt/seeds/cost_report_measures.csv` (now `dbt/seeds/gold/cost_report_measures_bridge.csv` and `dbt/seeds/features/cost_report_measures_recipe.csv`). It gives null for a zero or missing denominator.
- `int_impact_measures` carries the registry measures of source CMS_IPPS (`dbt/seeds/impact_measures.csv`, now `dbt/seeds/gold/impact_measures_bridge.csv`).
- `int_mup_measures` computes the registry measures of sources CMS-MUP-PROVIDER and CMS_MEDICARE_PROVIDER by the fields and rule in `dbt/seeds/mup_measures.csv` (now `dbt/seeds/gold/mup_measures_bridge.csv` and `dbt/seeds/features/mup_measures_recipe.csv`).
- `int_validation_program_values` carries the registry controls C285 to C288. All of them are comparison_only or exclude_primary.
- `int_validation_measure_windows` carries the PSI-90 and PSI-13 controls (E038, E039). They read the published IDs `PSI_90` and `PSI_13` (registry revision 3). The older IDs `PSI_90_SAFETY` and `PSI_13_POST_SEPSIS` have no alias.

## Alignment models

The alignment stage lines each staged source up with the hospital-year spine as of the HAI window start. Commit `e71779d` built it in silver. The Oct 10 2026 layer design moves it to the feature phase (see the [architecture diagram](architecture/architecture.png)).

- `int_spine_hai_outcomes` has one row per spine hospital-window and infection type (HAI_1 CLABSI to HAI_6 C. difficile). A row holds the published SIR, its bounds, the observed and predicted counts and the exposure, by exact measure ID, with tokens kept as text and the SIR footnote codes. No SIR is computed from the counts. The model holds and counts a part from a release after the last 2015-baseline review (Aug 13 2026), so 2022-baseline SIRs never join the series. It counts a part held in staging too. Each row has an alignment status (aligned, held_in_staging, not_in_source), the footnote of every part and the national benchmark category of the SIR, which `int_hai_hospital_windows` keeps as published. Each infection type stays separate. The model target is chosen before training. `int_hai_outcome_values` carries the HAI controls of the registry (C269 to C283 and the children of C283) by the part that `dbt/seeds/hai_outcome_measures.csv` (now `dbt/seeds/gold/hai_outcome_measures_bridge.csv`) names. It carries C269 to C274 as target candidates and C275 to C280, the same SIRs, as earlier-outcome forecasting candidates.
- `int_spine_care_compare_measures` has one row per spine hospital-window and Care Compare process or structure control in `dbt/seeds/registry_measure_sources.csv` (now `dbt/seeds/gold/registry_measure_sources_bridge.csv`), in families S13 to S16. Each control takes the latest measure window that ends before the HAI window starts. The overall rating (C284, excluded from the primary predictors) takes the latest Hospital General Information release dated before the start. The model holds the rating when two files on that date disagree. Values stay as published, with their age in months and no age limit. The spellings of C141 (for example `low` and `Low`) each map to one category. A test fails on any new spelling. A control that starts after a window (for example C161 to C173 from 2024) keeps the status `no_period_before_start`. The other statuses are aligned, held_in_staging and not_in_source.
- `int_spine_hospital_measures` does the same for the hospital finance and operations controls, by source and field. It takes the latest cost report whose period ends before the start (several reports ending that day are compared). It takes the IPPS impact value of the latest rule fiscal year that ends before the start (October to September, the most final stage first). It takes the latest Medicare inpatient data year and the latest occupational-mix survey. The model holds and counts values that disagree for one period. Occupational-mix copies of one survey (text and workbook twins in several rule-year files) within 0.0001 of each other count as one value.
- `int_spine_operations_measures` adds the operations controls as each registry field defines them:
  - HHS capacity comes from the weeks of the calendar year before the window. It is a ratio of sums for two fields, a sum of 7-day sums or a mean of 7-day averages. The model skips and counts a week where two HHS hospitals share a CCN.
  - ONC certified-EHR status, developers and product count come from the latest performance period.
  - Private-equity and REIT ownership comes from the latest owner release before the start (direct, indirect and partnership owners only). A blank is `not_reported`, never no.
  - Changes of ownership count those in the year before the start, with the completed months since the latest one.
- `int_spine_validation_measures` places the validation outcomes (HAC and VBP program years, HRRP, unplanned visits and 30-day mortality) against the own period of each HAI window. The project compares them with the window and never uses them to predict it, so no predictor model reads this table. The model takes the published window equal to the calendar year first and otherwise the one with the largest overlap. It places HAC and VBP years by the HAI measure period that HAC publishes for each fiscal year. PSI-90 and PSI-13 use the latest measurement period that ends strictly before the window starts. A prior PSI period has zero overlap days. Missing and held values keep their statuses.
- `int_county_adjacency` carries every column of the pinned gold `bridge_county_neighbor`, with one row per `edge_key`. It preserves vintages, self-links and isolated counties for spatial diagnostics. The build publishes it and `int_spine_validation_measures` with the feature tables as non-Feast training support. Consumers read their publication snapshot IDs inside `publication_reads.consistent_read`. Neither table is registered as a Feast view or accepted as a predictor reference. See [the feature store read contract](lakehouse_usage.md#feature-store).
- `int_spine_county_measures` adds the county context from PLACES, Medicare Geographic Variation, CDC WONDER, Mapping Medicare Disparities and RUCC through the county of the spine. It takes the latest data year (RUCC vintage) that ends before the window starts. PLACES counts only all-state measure-years and takes the latest release of each data year. WONDER 2018 to 2020 come from the single-race database. Connecticut matches a source on its old county or its planning region.
- `int_spine_county_context` adds ACS, SVI, SAIPE, SAHIE (all-group rows) and BLS (annual averages) by the same rule. It adds HPSA and MUA designations in force at the window start, rebuilt from their designation and withdrawal dates, with the score as published at the capture. An undated withdrawal holds the window. It also adds RUCA codes for the hospital ZIP.
- `int_spine_linkage` keeps the HUD residential share of the hospital ZIP and the hospital's service-area ZIPs and cases apart as linkage, never predictors.

## History models

The history models are type 2 slowly changing dimensions (SCD2), built in silver. The Oct 10 2026 layer design moves the history method to the feature phase. Gold keeps the snapshot and release rows. The publishers' own dates date the versions, so a clean checkout rebuilds the same versions. The models are:

- `int_hospital_pos_history` (Provider of Services attributes per snapshot)
- `int_hospital_hgi_history` (Hospital General Information descriptions per release, while the overall rating stays a measure)
- `int_hospital_ownership_history` (organization owners only, by enrollment, owner and role, per release)

The macro `dbt/macros/features/scd2.sql` starts a version when a tracked value changes or when a hospital or owner returns after a release without it. That release closes the earlier version. A release that lists one key twice with different values is held for that release. Some CMS owner and enrollment releases drop the leading zeros of the 10-digit associate IDs, so the row models pad them back and mark the row (`associate_ids_padded`). Ownership history bridges an owner missing from one release and counts it in `bridged_releases`. It never bridges a held release. POS and Hospital General Information history do not bridge. `dbt/tests/features/assert_history_versions_valid.sql` fails on overlapping windows or a second current version.
