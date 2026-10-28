-- One row naming the dbt run that wrote this build file and the DuckDB that wrote it, so a validation or a publish
-- binds the exact build it read (failure modes 639, 646 and 647). It depends on the kept models named below only;
-- ordering it after every table gold reads is later build-marker work. The validation table is left out: no model may
-- name it [599]; the contract suite fails a build where it is missing.
-- On a real build the launcher passes the run, the dbt tree hash, the bronze pins and the Git revision in the environment; a missing or
-- `unrecorded` value stops compilation (macros/build_provenance.sql, D1 section 10 item 8).
-- depends_on: {{ ref('int_county_adjacency_edges') }}
-- depends_on: {{ ref('int_change_of_ownership_holds') }}
{{ config(materialized='table') }}

{% set provenance = hai_build_provenance() %}

select
    '{{ invocation_id }}' as dbt_invocation_id,
    '{{ provenance.git_revision | replace("'", "''") }}' as git_revision,
    version() as duckdb_version,
    '{{ provenance.run_id | replace("'", "''") }}' as run_id,
    '{{ provenance.dbt_tree_sha256 | replace("'", "''") }}' as dbt_tree_sha256,
    '{{ provenance.bronze_pins_sha256 | replace("'", "''") }}' as bronze_pins_sha256,
    '{{ provenance.pins_json | replace("'", "''") }}' as pins_json
