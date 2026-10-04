{% macro hai_window_rows(table, entity) %}
{#- The rows of one HAI staging view with their window key, under either layout's column names [233]. -#}
select
    '{{ table }}' as bronze_table,
    nullif(trim({{ entity }}), '') as entity_id,
    nullif(trim(measure_id), '') as measure_id,
    try_strptime(coalesce(start_date, measure_start_date), '%m/%d/%Y')::date as window_start,
    try_strptime(coalesce(end_date, measure_end_date), '%m/%d/%Y')::date as window_end,
    measure_name,
    score,
    footnote,
    _member_sha256 as member_sha256,
    _object_key as object_key,
    is_label_held
from {{ ref('stg_' ~ table) }}
{% endmacro %}

{% macro hai_window_candidates(table, entity) %}
{#- Usable rows with their file's latest publication date, ranked within each window key [230] to [234]. -#}
with

window_rows as (
    {{ hai_window_rows(table, entity) }}
),

files as (
    select
        member_sha256,
        latest_publication_date
    from {{ ref('stg_bronze__files') }}
    where bronze_table = '{{ table }}'
),

dated as (
    select
        window_rows.*,
        files.latest_publication_date as release_date
    from window_rows
    inner join files on window_rows.member_sha256 = files.member_sha256
),

usable as (
    select *
    from dated
    where
        entity_id is not null
        and measure_id is not null
        and window_start is not null
        and window_end is not null
        and not is_label_held
        and release_date is not null
),

latest as (
    select
        *,
        max(release_date) over (partition by entity_id, measure_id, window_start, window_end) as latest_release_date,
        count(distinct member_sha256) over (partition by entity_id, measure_id, window_start, window_end) as file_count
    from usable
)

select
    *,
    count(*) over (partition by entity_id, measure_id, window_start, window_end) as latest_row_count,
    count(distinct member_sha256) over (partition by entity_id, measure_id, window_start, window_end) as latest_file_count
from latest
where release_date = latest_release_date
{% endmacro %}

{% macro hai_windows(table, entity) %}
{#- One row per entity, measure and window: the row from the latest dated file; conflicts are held instead [231] [232]. -#}
with

candidates as (
    {{ hai_window_candidates(table, entity) }}
)

select
    entity_id,
    measure_id,
    window_start,
    window_end,
    measure_name,
    score,
    footnote,
    member_sha256,
    object_key,
    release_date,
    file_count as release_file_count,
    entity_id || ':' || measure_id || ':' || window_start || ':' || window_end as window_key
from candidates
where latest_row_count = 1
{% endmacro %}

{% macro hai_window_holds(table, entity) %}
{#- The rows a window model leaves out, with the reason and row count [232] to [235]. -#}
with

window_rows as (
    {{ hai_window_rows(table, entity) }}
),

files as (
    select
        member_sha256,
        latest_publication_date
    from {{ ref('stg_bronze__files') }}
    where bronze_table = '{{ table }}'
),

reasons as (
    select
        window_rows.bronze_table,
        window_rows.entity_id,
        window_rows.measure_id,
        case
            when window_rows.entity_id is null or window_rows.measure_id is null then 'no_key'
            when window_rows.window_start is null or window_rows.window_end is null then 'unparsed_date'
            when window_rows.is_label_held then 'label_held'
            when files.latest_publication_date is null then 'undated_release'
        end as hold_reason
    from window_rows
    left join files on window_rows.member_sha256 = files.member_sha256
),

row_holds as (
    select
        bronze_table,
        hold_reason,
        case when hold_reason = 'no_key' then null else entity_id end as entity_id,
        case when hold_reason = 'no_key' then null else measure_id end as measure_id,
        cast(null as date) as window_start,
        cast(null as date) as window_end,
        count(*) as row_count
    from reasons
    where hold_reason is not null
    group by all
),

candidates as (
    {{ hai_window_candidates(table, entity) }}
),

window_holds as (
    select
        bronze_table,
        case when max(latest_file_count) > 1 then 'same_date_conflict' else 'repeated_in_file' end as hold_reason,
        entity_id,
        measure_id,
        window_start,
        window_end,
        count(*) as row_count
    from candidates
    where latest_row_count > 1
    group by
        bronze_table,
        entity_id,
        measure_id,
        window_start,
        window_end
)

select * from row_holds
union all by name
select * from window_holds
{% endmacro %}
