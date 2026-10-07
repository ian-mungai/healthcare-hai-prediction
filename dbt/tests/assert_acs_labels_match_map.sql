-- Fails for each mapped export column whose label in that vintage's file differs from the label the reviewed map
-- recorded, or that the file does not publish: a code that changes meaning never carries the concept [420].
with

variable_map as (
    select
        concept_id,
        bronze_table,
        vintage,
        column_name,
        published_label
    from {{ ref('acs_variable_map') }}
    where status = 'mapped' and not starts_with(bronze_table, 'acs_summary_')
),

files as (
    select
        files.bronze_table,
        files.canonical_object_key,
        periods.vintage
    from {{ ref('stg_bronze__files') }} as files
    inner join {{ ref('geography_file_periods') }} as periods
        on
            files.bronze_table = periods.bronze_table
            and files.member_sha256 = periods.member_sha256
),

labels as (
    select
        _object_key as object_key,
        column_name,
        original_label
    from {{ source('bronze', 'column_map') }}
)

select
    variable_map.concept_id,
    variable_map.bronze_table,
    variable_map.vintage,
    variable_map.column_name,
    labels.original_label
from variable_map
inner join files
    on
        variable_map.bronze_table = files.bronze_table
        and variable_map.vintage = files.vintage
left join labels
    on
        files.canonical_object_key = labels.object_key
        and variable_map.column_name = lower(labels.column_name)
where labels.original_label is distinct from variable_map.published_label
