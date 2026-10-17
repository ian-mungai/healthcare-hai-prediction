{{ config(tags=['silver'], materialized='table') }}
select
    '{{ invocation_id }}' as dbt_invocation_id,
    'tiny' as git_revision,
    version() as duckdb_version
