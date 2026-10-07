-- Fails for a concept without exactly one map row for a loaded vintage of its table, and for a map row naming no loaded
-- file: every concept and vintage is mapped or held with a reason, never silently missing [423].
with

variable_map as (
    select
        concept_id,
        bronze_table,
        vintage,
        status,
        reason
    from {{ ref('acs_variable_map') }}
),

concepts as (
    select distinct
        concept_id,
        bronze_table
    from variable_map
),

files as (
    select distinct
        periods.bronze_table,
        periods.vintage
    from {{ ref('geography_file_periods') }} as periods
    inner join {{ ref('stg_bronze__files') }} as files
        on
            periods.bronze_table = files.bronze_table
            and periods.member_sha256 = files.member_sha256
    where starts_with(periods.bronze_table, 'acs_')
),

expected as (
    select
        concepts.concept_id,
        concepts.bronze_table,
        files.vintage
    from concepts
    inner join files on concepts.bronze_table = files.bronze_table
),

counted as (
    select
        expected.concept_id,
        expected.bronze_table,
        expected.vintage,
        count(variable_map.concept_id) as map_rows
    from expected
    left join variable_map
        on
            expected.concept_id = variable_map.concept_id
            and expected.bronze_table = variable_map.bronze_table
            and expected.vintage = variable_map.vintage
    group by
        expected.concept_id,
        expected.bronze_table,
        expected.vintage
    having count(variable_map.concept_id) <> 1
),

stale as (
    select
        variable_map.concept_id,
        variable_map.bronze_table,
        variable_map.vintage,
        0 as map_rows
    from variable_map
    left join files
        on
            variable_map.bronze_table = files.bronze_table
            and variable_map.vintage = files.vintage
    where files.vintage is null
),

unexplained as (
    select
        concept_id,
        bronze_table,
        vintage,
        -1 as map_rows
    from variable_map
    where status not in ('mapped', 'held') or (status = 'held' and nullif(trim(reason), '') is null)
)

select * from counted
union all
select * from stale
union all
select * from unexplained
