-- One row naming the dbt run that wrote this build file and the DuckDB that wrote it, so a validation or a publish
-- binds the exact build it read (silver step 6, failure modes 639, 646 and 647,
-- plans/silver_processed_zone_20261009/plan.md). It depends on the kept models named below only; ordering it after
-- every table gold reads is wave 2 build-marker work (amendment A28). The validation table is left out: no model may
-- name it [599]; the contract suite fails a build where it is missing.
-- depends_on: {{ ref('int_county_adjacency_edges') }}
-- depends_on: {{ ref('int_change_of_ownership_holds') }}
{{ config(materialized='table') }}

select
    '{{ invocation_id }}' as dbt_invocation_id,
    '{{ var("git_revision", "unrecorded") }}' as git_revision,
    version() as duckdb_version
