-- Fails for each HAI or Care Compare window hold whose member_sha256s or row_count differs from the rows it holds, and for
-- each expected hold that is missing or unexpected (failure mode 792). The held rows are rebuilt here from the staging
-- rows (care_window_rows, with each window model's table, entity, measure, dates and columns) and the files' release
-- dates: a row hold by the first reason that applies (no key, unparsed date, held label, undated release) and a window
-- hold where the latest release date of a window holds more than one row. The checksums must be exactly the sorted
-- distinct files of those rows, so a real file of the same table that holds none of them is rejected.
with

window_rows as (
    select
        *,
        'int_hai_window_holds' as hold_model
    from ({{ care_window_rows(
        'cms_hai_hospital',
        "coalesce(facility_id, provider_id)",
        'measure_id',
        'coalesce(start_date, measure_start_date)',
        'coalesce(end_date, measure_end_date)',
        none
    ) }})
    union all by name
    select
        *,
        'int_hai_window_holds' as hold_model
    from ({{ care_window_rows(
        'cms_hai_state',
        "state",
        'measure_id',
        'coalesce(start_date, measure_start_date)',
        'coalesce(end_date, measure_end_date)',
        none
    ) }})
    union all by name
    select
        *,
        'int_hai_window_holds' as hold_model
    from ({{ care_window_rows(
        'cms_hai_national',
        "'US'",
        'measure_id',
        'coalesce(start_date, measure_start_date)',
        'coalesce(end_date, measure_end_date)',
        none
    ) }})
    union all by name
    select
        *,
        'int_cc_window_holds' as hold_model
    from ({{ care_window_rows(
        'cms_cc_timely_and_effective_care_hospital',
        "coalesce(facility_id, provider_id)",
        'measure_id',
        'coalesce(start_date, measure_start_date)',
        'coalesce(end_date, measure_end_date)',
        none
    ) }})
    union all by name
    select
        *,
        'int_cc_window_holds' as hold_model
    from ({{ care_window_rows('cms_cc_maternal_health_hospital', "facility_id", 'measure_id', 'start_date', 'end_date', none) }})
    union all by name
    select
        *,
        'int_cc_window_holds' as hold_model
    from ({{ care_window_rows(
        'cms_cc_hcahps_hospital',
        "coalesce(facility_id, provider_id)",
        'hcahps_measure_id',
        'coalesce(start_date, measure_start_date)',
        'coalesce(end_date, measure_end_date)',
        [['measure_name', 'hcahps_question']]
    ) }})
),

files as (
    select
        bronze_table,
        member_sha256,
        latest_publication_date
    from {{ ref('stg_bronze__files') }}
),

dated as (
    select
        window_rows.hold_model,
        window_rows.bronze_table,
        window_rows.entity_id,
        window_rows.measure_id,
        window_rows.window_start,
        window_rows.window_end,
        window_rows.member_sha256,
        files.latest_publication_date as release_date,
        case
            when window_rows.entity_id is null or window_rows.measure_id is null then 'no_key'
            when window_rows.window_start is null or window_rows.window_end is null then 'unparsed_date'
            when window_rows.is_label_held then 'label_held'
            when files.latest_publication_date is null then 'undated_release'
        end as row_reason
    from window_rows
    left join files
        on
            window_rows.bronze_table = files.bronze_table
            and window_rows.member_sha256 = files.member_sha256
),

row_holds as (
    select
        hold_model,
        bronze_table,
        row_reason as hold_reason,
        case when row_reason = 'no_key' then null else entity_id end as entity_id,
        case when row_reason = 'no_key' then null else measure_id end as measure_id,
        cast(null as date) as window_start,
        cast(null as date) as window_end,
        count(*) as row_count,
        list_sort(list_distinct(array_agg(member_sha256))) as member_sha256s
    from dated
    where row_reason is not null
    group by all
),

latest_rows as (
    select *
    from dated
    where row_reason is null
    qualify release_date = max(release_date) over (partition by hold_model, bronze_table, entity_id, measure_id, window_start, window_end)
),

window_holds as (
    select
        hold_model,
        bronze_table,
        case when count(distinct member_sha256) > 1 then 'same_date_conflict' else 'repeated_in_file' end as hold_reason,
        entity_id,
        measure_id,
        window_start,
        window_end,
        count(*) as row_count,
        list_sort(list_distinct(array_agg(member_sha256))) as member_sha256s
    from latest_rows
    group by
        hold_model,
        bronze_table,
        entity_id,
        measure_id,
        window_start,
        window_end
    having count(*) > 1
),

expected as (
    select * from row_holds
    union all by name
    select * from window_holds
),

actual as (
    select
        'int_hai_window_holds' as hold_model,
        bronze_table,
        hold_reason,
        entity_id,
        measure_id,
        window_start,
        window_end,
        row_count,
        member_sha256s
    from {{ ref('int_hai_window_holds') }}
    union all
    select
        'int_cc_window_holds' as hold_model,
        bronze_table,
        hold_reason,
        entity_id,
        measure_id,
        window_start,
        window_end,
        row_count,
        member_sha256s
    from {{ ref('int_cc_window_holds') }}
)

select
    expected.row_count as expected_rows,
    actual.row_count,
    expected.member_sha256s as expected_members,
    actual.member_sha256s,
    coalesce(expected.hold_model, actual.hold_model) as hold_model,
    coalesce(expected.bronze_table, actual.bronze_table) as bronze_table,
    coalesce(expected.hold_reason, actual.hold_reason) as hold_reason,
    coalesce(expected.entity_id, actual.entity_id) as entity_id,
    coalesce(expected.measure_id, actual.measure_id) as measure_id
from expected
full outer join actual
    on
        expected.hold_model = actual.hold_model
        and expected.bronze_table = actual.bronze_table
        and expected.hold_reason = actual.hold_reason
        and expected.entity_id is not distinct from actual.entity_id
        and expected.measure_id is not distinct from actual.measure_id
        and expected.window_start is not distinct from actual.window_start
        and expected.window_end is not distinct from actual.window_end
where
    expected.hold_model is null
    or actual.hold_model is null
    or expected.row_count is distinct from actual.row_count
    or expected.member_sha256s is distinct from actual.member_sha256s
