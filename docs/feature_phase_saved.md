---
title: Saved for the Feature Phase
description: The saved silver models, now the feature phase in dbt/models/features, as described when they were silver models.
last_updated: 2026-10-22
---

# Saved for the Feature Phase

These models are the feature phase. They sit in `dbt/models/features/` with their macros in `dbt/macros/features/` and their
singular tests in `dbt/tests/features/`. dbt builds them as views after gold, never inside the gold build. They read gold
facts, dimensions and bridges instead of silver. The dbt tag `features` marks them. The gold build selections exclude that
tag. A command with no selection uses the default selector in `dbt/selectors.yml`, which leaves it out. Select them with
`--select tag:features`. The spine and the case-mix index models are tables in the build's DuckDB file only. The formula
recipes are the seeds in `dbt/seeds/features/`, tagged `features` too, so the gold selections leave them out and the feature
build loads them. `features/` keeps the archived Great Expectations suites and `archive_manifest.json`.
`scripts/lakehouse/feature_sources.py` fails when a feature model, macro or test reads a silver model, a retired seed or
bronze. Its text pass is the fast first line: it reads the references in the text (a quoted `ref()` or `source()`, a silver relation named in SQL text) and rejects any
reference it cannot resolve there, such as one built from a Jinja variable. Its resolved pass does not depend on the spelling of a read. It has two layers. The second starts only when the first finds nothing: admission (below) runs on a
parse and builds nothing. Any problem it reports ends the check before a dbt build, compile, seed or query starts. The build layer builds the feature phase in
two stages (`feature_columns.run_features`). Stage 1 is no dbt run: the check builds the 15 filter tables itself in DuckDB from a database that holds the gold-contract
tables, the raw bridge included (`create table <filter> as select * from bridge_measure_control where source_seed = ?`, the seed taken from the file name), so no hook,
macro or configuration of the project can change their rows. Stage 2 runs every other feature model with dbt on a DuckDB file that holds only the relations a feature model
may read (the published gold tables of the frozen contract without `bridge_measure_control`, the seeds in `dbt/seeds/features`, the live reference seeds directly under
`dbt/seeds` and the filter tables), with external access off. The scratch copy of the project runs with `on-run-start` and `on-run-end` emptied. The pass reads the
selection of `dbt ls --select tag:features` and binds the compiled query of every selected model and test there, the generic tests that a YAML file in a feature folder
declares included (seed tests too), each on a database that holds only the relations pinned for that node. It fails by name on a selected node of any other type
(snapshot, analysis, unit test, source), on a macro that overrides `ref`, `source`, a materialization or a macro of dbt or dbt-duckdb, on a filter model whose compiled query is not
the template, on a top-level key of `dbt/dbt_project.yml` outside `feature_sources.PROJECT_KEYS` (`dispatch` and `query-comment` among them), on a run hook other than the two
pinned build marker hooks (`feature_sources.RUN_HOOKS`) and on a relation in the stage-2 database after the run and the compile that neither the restricted database nor a
feature model gives. It fails too on a relation whose rows (count and digest) differ from before the run. A read of silver, bronze, a retired seed, a bridge seed, the raw bridge or any other
relation fails because the relation does not exist, whether it is written in upper case, quoted, aliased, counted, built with `query_table`, `var()`, a macro or an
adapter relation. A table function that reads a file or another database fails too. The pass also reads the dependencies in the parsed dbt manifest.

SQL that runs outside the compiled query of a node (a `run_query()` that rewrites a filter table or creates, reads and drops the raw bridge, a `statement` block,
`adapter.execute`, a `sql_header`) leaves no trace to find afterwards, so the check does not look for it: it admits only what is in a closed list
(`scripts/lakehouse/feature_allowlist.py`, run before anything builds on a parse of a scratch copy of the project). The scope is every selected node, every seed and the two gold
models that the shell build runs. The lists:

- **Jinja** of each node and of each project macro it reaches (transitively, from `depends_on.macros`), parsed with dbt's own Jinja: the constructs of `NODE_TYPES` only (text,
  output, `set` of a name, `if`, `for`, `not`, comparison, `~`, a conditional expression, list, tuple, `join`, `loop.first` and `loop.last`, macro blocks); calls only to `ref`,
  `config` (with `alias`, `column_types`, `contract`, `materialized`, `meta` or `tags` only), `return` and the macros the code depends on. Every name the code reads without
  declaring it must be one of those, so `run_query`, `statement`, call blocks, `do`, `adapter`, `execute`, `target`, `var`, `env_var`, `modules`, `log` and dict literals fail
  with the node, file and line. Nothing environment-dependent can reach a `ref`, so a `ref` resolves the same at parse and at run time.
- **dbt macros** a node may reach, by exact id (`PACKAGE_MACROS`): the generic tests `unique`, `not_null` and `accepted_values` with their default implementations, plus
  the `get_where_subquery` and `should_store_failures` macros that they call. Any other macro of dbt, dbt-duckdb or a package fails. A project macro with the name of a dbt context
  function (`config`, `var`, `return`, `log`, `local_md5` and the rest of dbt's context) fails because it would replace that function for every node.
- **Config** resolved by dbt (node, folder and project levels merged): only the keys of `INERT` with dbt's default value (for example `enabled`, `grants`, `persist_docs`,
  `on_schema_change`, `where`, `severity`) and the keys of `FREE` with the values their rule accepts (`materialized` view or table, `tags`, `meta`, `alias`, `contract`,
  `column_types`). A `sql_header`, `persist_docs`, `grants` or any other key fails. The SQL text that a column `data_type` or a seed `column_types` entry carries into a
  create-table statement must be a plain type text. A constraint fails.
- **YAML** property files of the project: a Jinja string in a generic test argument or a description may call `ref` only (dbt renders it at parse time).
- **Run hooks:** the SHA-256 of the code of each hook macro and of each project macro they reach is pinned (`HOOK_MACRO_SHA256`). A change fails the check and names the macro. The
  scratch builds run with both hook lists emptied.
- **Hooks:** a `pre-hook` or `post-hook` on a node in scope or on any seed or a run hook that a package adds fails at admission.
- **Inputs:** `dbt/models/features/_feature_inputs.json` lists, for each feature model and singular test, the nodes it may `ref()` (gold facts, seeds, other feature models and the
  filter tables of its own sources). A `ref()` outside the list, a model or test without an entry and an entry without a node fail at admission. A generic test may read the
  node it tests and nothing else. After the build each model and test is bound on a database that holds only its pinned relations, so a filter table of another source also
  fails when it is read by a relation name or `query_table`. To change an entry, edit the file by hand and have the change reviewed.

A change that needs a construct outside a list fails with the construct named; widen the list in `feature_allowlist.py` only after review.
`feature_columns --check` runs the same admission before it builds. After the build, the digest of every relation, views included, is compared. A relation that cannot be read fails the check.

The shared control bridge `bridge_measure_control` holds the controls of all 15 retired seeds. A feature model may read it only for its own seed. That rule is built
into the stages and not read from SQL. The bridge filter models in `dbt/models/features/bridge` are `feature_bridge_<retired seed>.sql`, one per retired seed (15). Each is
exactly the template `feature_sources.BRIDGE_FILTER` with its seed: a table `select * from bridge_measure_control where source_seed = '<seed>'`. The text pass fails on a file
that differs from the template, a file in that folder that is no filter model, a retired seed without a filter model and a filter model that no other feature model or
test reads. Every other feature model and test reads the filter model of its seed and never `bridge_measure_control`. A gold dimension that the frozen contract gives no
columns for is not in that file, so a feature model that reads one fails until its columns are added to the contract. Every
feature model, the filter models included, has an enforced column contract (`+contract` for the folder in `dbt/dbt_project.yml`). The file `_features__models.yml` documents every
output column with its data type and description. `scripts/lakehouse/feature_columns.py` compares the YAML with the columns the models give on
empty gold tables, with the contract switched off, so an undocumented or stale column fails the check on its own. The enumerated output columns
(status, type, rule, rule stage, review decision and the fields of the closed lists) have `accepted_values` tests with the values each model can
produce. The identity of the controls of the seven lookup models (Care Compare, validation, hospital, operations, HAI outcomes and the two county models) is frozen in
`dbt/seeds/features/frozen_*.csv`, generated once from the retired seeds at `e918ed7`. The tests `assert_*_control_identity` compare it with the controls each model
carries, with the gold control bridge and with the formula recipes, as tuples of source, control, measure and value column (for the county lookups,
`frozen_county_controls.csv`), so a control swapped for another or redirected to another measure of the same fact fails even when the count stays the same. The descriptions below stay as written when the
models were silver models.

The case-mix index models choose one CMI per hospital and rule year from the year's best rule stage; disagreeing files are held in
`int_cmi_holds`. `int_hospital_spine` has one row per hospital and calendar-year HAI window, as of the window start: the
Provider of Services snapshot that ends in the 12 months before the window and the CMI of the fiscal year that ends before
it (`int_cmi_hospital_data_years`), with the primary and sensitivity population flags. A window missing its own snapshot
takes its state and county from the same hospital's other snapshots within 24 months, then the enrollment file, then the
CCN's state code. A county can also come from the hospital ZIP when the HUD crosswalk places that ZIP in one county only; a
ZIP split across counties gives none. Classification is filled only when the neighboring snapshots agree. Each filled field
names its source. Connecticut windows also carry their planning region when the ZIP lies in one region.

The registry-map seeds of commit `e918ed7` are retired; the same mappings live in the gold control bridges and recipes named
below, which the feature models read instead.
The rewritten models keep their grain and output columns, except three. `int_spine_care_compare_measures` gives `fact` (the gold
table that holds the value) instead of `family` and `source_model`. `int_spine_validation_measures` gives `fact` instead of
`source_model`. `int_hai_outcome_values` has no `original_decision`: gold does not carry the target candidates, so the owner decides
whether gold or the training configuration names them. A closed control with no measure key (the validation payment adjustment
C288 and the HHS mappings C001 and C040) is not carried.
`int_registry_measure_windows` carried each registry measure of the Care Compare timely and effective care, maternal health and
HCAHPS families by the exact measure ID in `dbt/seeds/registry_measure_sources.csv` (now
`dbt/seeds/gold/registry_measure_sources_bridge.csv`).
`int_cost_report_measures` computed each registry cost-report measure by the
columns and rule in `dbt/seeds/cost_report_measures.csv` (now `dbt/seeds/gold/cost_report_measures_bridge.csv` and
`dbt/seeds/features/cost_report_measures_recipe.csv`) and gave null for a zero or missing denominator. `int_impact_measures` carried the registry measures of source CMS_IPPS (`dbt/seeds/impact_measures.csv`, now
`dbt/seeds/gold/impact_measures_bridge.csv`). `int_mup_measures` computed the registry
measures of sources CMS-MUP-PROVIDER and CMS_MEDICARE_PROVIDER by the fields and rule in `dbt/seeds/mup_measures.csv` (now
`dbt/seeds/gold/mup_measures_bridge.csv` and `dbt/seeds/features/mup_measures_recipe.csv`). `int_validation_program_values` carried the registry controls (C285 to C288); all
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
`dbt/macros/features/scd2.sql` starts a version when a tracked value changes or when a hospital or owner returns after a release
without it; that release closes the earlier version. A release that lists one key twice with different values is held
for that release. Some CMS owner and enrollment releases drop the leading zeros of the 10-digit associate IDs, so the
row models pad them back and mark the row (`associate_ids_padded`). Ownership history bridges an owner missing from one
release and counts it in `bridged_releases` (owner decision, October 10 2026). A held release is never bridged; POS and
Hospital General Information history do not bridge. `dbt/tests/features/assert_history_versions_valid.sql` fails on overlapping windows or a
second current version.
