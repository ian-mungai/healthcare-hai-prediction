-- One row naming the dbt run that wrote this build file and the DuckDB that wrote it, built after the tables gold reads,
-- so a validation or a publish binds the exact build it read (silver step 6, failure modes 639, 646 and 647,
-- plans/silver_processed_zone_20261009/plan.md). The validation table is left out: no model may name it [599]; the
-- contract suite fails a build where it is missing.
-- depends_on: {{ ref('int_hospital_spine') }}
-- depends_on: {{ ref('int_spine_hai_outcomes') }}
-- depends_on: {{ ref('int_spine_care_compare_measures') }}
-- depends_on: {{ ref('int_spine_hospital_measures') }}
-- depends_on: {{ ref('int_spine_operations_measures') }}
-- depends_on: {{ ref('int_spine_county_measures') }}
-- depends_on: {{ ref('int_spine_county_context') }}
-- depends_on: {{ ref('int_spine_linkage') }}
-- depends_on: {{ ref('int_county_adjacency_edges') }}
{{ config(materialized='table') }}

select
    '{{ invocation_id }}' as dbt_invocation_id,
    '{{ var("git_revision", "unrecorded") }}' as git_revision,
    version() as duckdb_version
