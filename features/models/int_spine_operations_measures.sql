-- One row per spine hospital-window and operations control (HHS capacity, ONC certified EHR, ownership and change of
-- ownership): HHS weeks of the calendar year before the window, the latest ONC performance period, the latest owner
-- release and the change-of-ownership events before the window start, each as the registry's exact field defines it,
-- with its age in months and an alignment status (alignment step AL3b, failure modes 587 to 596,
-- plans/alignment_20261008/failure_modes_al3b.md).
{{ config(materialized='table') }}

with

spine as (
    select
        ccn,
        window_year,
        window_start,
        spine_key,
        is_primary_population,
        is_sensitivity_population,
        is_connecticut,
        is_maryland,
        make_date(window_year - 1, 1, 1) as prior_year_start,
        make_date(window_year - 1, 12, 31) as prior_year_end,
        window_start - interval 1 year as change_window_start
    from {{ ref('int_hospital_spine') }}
),

hhs_controls as (
    select
        measure_control,
        review_decision,
        string_split(coalesce(fields, ''), ' ') as field_list
    from {{ ref('hhs_onc_measures') }}
    where source_model = 'int_hhs_capacity_weeks'
),

controls as (
    select
        measure_control,
        review_decision,
        'hhs_capacity' as measure_source
    from hhs_controls
    union all
    select
        measure_control,
        review_decision,
        'onc' as measure_source
    from {{ ref('hhs_onc_measures') }}
    where source_model = 'int_onc_chpl_linkage_rows'
    union all
    select
        measure_control,
        review_decision,
        'ownership' as measure_source
    from {{ ref('ownership_measures') }}
),

hhs_long as (
    -- Every published 7-day average or sum, one row per CCN, week, HHS hospital and field.
    unpivot (
        select
            ccn,
            collection_week,
            hospital_pk,
            columns('_7_day_(avg|sum)$')
        from {{ ref('int_hhs_capacity_weeks') }}
        where ccn is not null
    )
    on columns('_7_day_(avg|sum)$')
    into name field value field_value
),

hhs_weeks as (
    -- A week with more than one HHS hospital for the CCN is skipped and counted [589].
    select
        ccn,
        collection_week,
        count(distinct hospital_pk) as hospitals
    from {{ ref('int_hhs_capacity_weeks') }}
    where ccn is not null
    group by
        ccn,
        collection_week
),

hhs_fields as (
    select
        ccn,
        collection_week,
        field,
        -- Exact decimals, so sums do not depend on the order the engine adds them in [596].
        max(field_value)::decimal(38, 6) as field_value
    from hhs_long
    group by
        ccn,
        collection_week,
        field
),

hhs_values as (
    -- Each control's one or two fields per week [588].
    select
        hhs_weeks.ccn,
        hhs_weeks.collection_week,
        hhs_weeks.hospitals,
        hhs_controls.measure_control,
        first_field.field_value as first_value,
        second_field.field_value as second_value,
        len(hhs_controls.field_list) as field_count,
        hhs_controls.field_list[1] like '%\_7\_day\_sum' escape '\' as is_weekly_sum
    from hhs_weeks
    cross join hhs_controls
    left join hhs_fields as first_field
        on
            hhs_weeks.ccn = first_field.ccn
            and hhs_weeks.collection_week = first_field.collection_week
            and hhs_controls.field_list[1] = first_field.field
    left join hhs_fields as second_field
        on
            hhs_weeks.ccn = second_field.ccn
            and hhs_weeks.collection_week = second_field.collection_week
            and hhs_controls.field_list[2] = second_field.field
    where hhs_controls.field_list[1] <> ''
),

hhs as (
    -- The calendar year before the window [587]: ratio of sums, a sum of weekly sums or a mean of weekly averages.
    select
        spine.spine_key,
        hhs_values.measure_control,
        spine.prior_year_start as period_start,
        spine.prior_year_end as period_end,
        'hhs_capacity' as measure_source,
        null as value_text,
        false as is_conflict,
        case
            when any_value(hhs_values.field_count) = 2
                then
                    sum(hhs_values.first_value) filter (where hhs_values.hospitals = 1 and hhs_values.second_value is not null)
                    / nullif(sum(hhs_values.second_value) filter (where hhs_values.hospitals = 1 and hhs_values.first_value is not null), 0)
            when bool_or(hhs_values.is_weekly_sum) then sum(hhs_values.first_value) filter (where hhs_values.hospitals = 1)
            else avg(hhs_values.first_value) filter (where hhs_values.hospitals = 1)
        end as value_number,
        count(*) filter (
            where
            hhs_values.hospitals = 1
            and hhs_values.first_value is not null
            and (hhs_values.field_count = 1 or hhs_values.second_value is not null)
        ) as unit_count,
        count(*) filter (where hhs_values.hospitals > 1) as skipped_count
    from spine
    inner join hhs_values
        on
            spine.ccn = hhs_values.ccn
            and hhs_values.collection_week between spine.prior_year_start and spine.prior_year_end
    group by
        spine.spine_key,
        hhs_values.measure_control,
        spine.prior_year_start,
        spine.prior_year_end
),

onc_periods as (
    -- One performance period per facility; the latest that ends before the start is chosen below [590] [591].
    select
        ccn,
        start_date,
        end_date,
        count(distinct meets_criteria_for_promoting_interoperability_of_ehrs) as flag_versions,
        case when bool_or(meets_criteria_for_promoting_interoperability_of_ehrs) then 'Y' else 'N' end as flag,
        string_agg(distinct developer_name, '; ' order by developer_name) as developers,
        count(distinct developer_name) as developer_count,
        count(distinct chpl_id) as products,
        string_agg(distinct cehrt_id, '; ' order by cehrt_id) as cehrt_ids
    from {{ ref('int_onc_chpl_linkage_rows') }}
    where ccn is not null
    group by
        ccn,
        start_date,
        end_date
),

onc_latest as (
    select
        onc_periods.ccn,
        onc_periods.start_date,
        onc_periods.end_date,
        onc_periods.flag_versions,
        onc_periods.flag,
        onc_periods.developers,
        onc_periods.developer_count,
        onc_periods.products,
        onc_periods.cehrt_ids,
        spine.spine_key,
        row_number() over (partition by spine.spine_key order by onc_periods.end_date desc) as recency
    from spine
    inner join onc_periods
        on
            spine.ccn = onc_periods.ccn
            and spine.window_start > onc_periods.end_date
),

onc as (
    select
        onc_latest.spine_key,
        controls.measure_control,
        onc_latest.start_date as period_start,
        onc_latest.end_date as period_end,
        'onc' as measure_source,
        1 as unit_count,
        0 as skipped_count,
        case controls.measure_control
            when 'C071' then case when onc_latest.flag_versions = 1 then onc_latest.flag end
            when 'C072' then onc_latest.developers
            when 'C074' then onc_latest.cehrt_ids
        end as value_text,
        case controls.measure_control
            when 'C072' then onc_latest.developer_count
            when 'C073' then onc_latest.products
        end::double as value_number,
        controls.measure_control = 'C071' and onc_latest.flag_versions > 1 as is_conflict
    from onc_latest
    inner join controls on controls.measure_source = 'onc'
    where onc_latest.recency = 1
),

enrollment_ccns as (
    -- Owner rows reach a hospital only through an enrollment ID with a CCN [593].
    select distinct
        enrollment_id,
        ccn
    from {{ ref('int_hospital_enrollment_rows') }}
    where
        enrollment_id is not null
        and ccn is not null
),

owner_links as (
    -- Each owner release and the hospitals its enrollments reach.
    select distinct
        owners.member_sha256,
        enrollment_ccns.ccn,
        periods.period_start,
        periods.period_end
    from {{ ref('int_hospital_owner_rows') }} as owners
    inner join enrollment_ccns on owners.enrollment_id = enrollment_ccns.enrollment_id
    inner join {{ ref('ownership_release_periods') }} as periods on owners.member_sha256 = periods.member_sha256
),

owner_latest as (
    select
        spine.spine_key,
        spine.ccn,
        owner_links.member_sha256,
        owner_links.period_start,
        owner_links.period_end,
        row_number() over (
            partition by spine.spine_key
            order by owner_links.period_end desc, owner_links.member_sha256 asc
        ) as recency
    from spine
    inner join owner_links
        on
            spine.ccn = owner_links.ccn
            and spine.window_start > owner_links.period_end
),

owner_flags as (
    -- Qualifying ownership roles only (direct, indirect and partnership interests) [592].
    select
        owner_latest.spine_key,
        owner_latest.period_start,
        owner_latest.period_end,
        owner_rows.enrollment_id,
        owner_rows.associate_id_owner,
        owner_rows.role_code in ('34', '35', '38', '39') and owner_rows.private_equity_company_owner as is_private_equity,
        owner_rows.role_code in ('34', '35', '38', '39') and owner_rows.reit_owner as is_reit
    from owner_latest
    inner join {{ ref('int_hospital_owner_rows') }} as owner_rows on owner_latest.member_sha256 = owner_rows.member_sha256
    inner join enrollment_ccns
        on
            owner_rows.enrollment_id = enrollment_ccns.enrollment_id
            and owner_latest.ccn = enrollment_ccns.ccn
    where owner_latest.recency = 1
),

owners as (
    -- Y when any qualifying owner reports the flag, otherwise not_reported, never N [592].
    select
        owner_flags.spine_key,
        controls.measure_control,
        owner_flags.period_start,
        owner_flags.period_end,
        'ownership' as measure_source,
        0 as skipped_count,
        false as is_conflict,
        case controls.measure_control
            when 'L005' then string_agg(distinct owner_flags.enrollment_id, '; ' order by owner_flags.enrollment_id)
            when 'C067' then case when bool_or(owner_flags.is_private_equity) then 'Y' else 'not_reported' end
            else case when bool_or(owner_flags.is_reit) then 'Y' else 'not_reported' end
        end as value_text,
        case controls.measure_control
            when 'L005' then count(distinct owner_flags.enrollment_id)
            when 'C067' then count(distinct owner_flags.associate_id_owner) filter (where owner_flags.is_private_equity)
            else count(distinct owner_flags.associate_id_owner) filter (where owner_flags.is_reit)
        end::double as value_number,
        count(*) as unit_count
    from owner_flags
    inner join controls on controls.measure_control in ('C067', 'C068', 'L005')
    group by
        owner_flags.spine_key,
        controls.measure_control,
        owner_flags.period_start,
        owner_flags.period_end
),

chow_parties as (
    select
        event_key,
        effective_date,
        chow_type_code,
        ccn_buyer as ccn
    from {{ ref('int_change_of_ownership_rows') }}
    union all
    select
        event_key,
        effective_date,
        chow_type_code,
        ccn_seller as ccn
    from {{ ref('int_change_of_ownership_rows') }}
),

chow_events as (
    select distinct
        event_key,
        effective_date,
        chow_type_code,
        ccn
    from chow_parties
    where
        ccn is not null
        and effective_date is not null
),

chow as (
    -- C069: distinct events in [start - 1 year, start), every type listed; C070: completed months since the latest [594].
    select
        spine.spine_key,
        controls.measure_control,
        'ownership' as measure_source,
        0 as skipped_count,
        false as is_conflict,
        case when controls.measure_control = 'C069' then spine.change_window_start end::date as period_start,
        case
            when controls.measure_control = 'C069' then spine.window_start - interval 1 day
            else max(chow_events.effective_date)
        end::date as period_end,
        case
            when controls.measure_control = 'C069'
                then
                    string_agg(distinct chow_events.chow_type_code, '; ' order by chow_events.chow_type_code)
                    filter (where chow_events.effective_date >= spine.change_window_start)
        end as value_text,
        case
            when controls.measure_control = 'C069'
                then count(distinct chow_events.event_key) filter (where chow_events.effective_date >= spine.change_window_start)
            else date_sub('month', max(chow_events.effective_date), spine.window_start)
        end::double as value_number,
        count(distinct chow_events.event_key) as unit_count
    from spine
    inner join chow_events
        on
            spine.ccn = chow_events.ccn
            and spine.window_start > chow_events.effective_date
    inner join controls on controls.measure_control in ('C069', 'C070')
    group by
        spine.spine_key,
        spine.window_start,
        spine.change_window_start,
        controls.measure_control
),

chosen as (
    select
        spine_key,
        measure_source,
        measure_control,
        period_start,
        period_end,
        value_text,
        value_number,
        unit_count,
        skipped_count,
        is_conflict
    from hhs
    union all
    select
        spine_key,
        measure_source,
        measure_control,
        period_start,
        period_end,
        value_text,
        value_number,
        unit_count,
        skipped_count,
        is_conflict
    from onc
    union all
    select
        spine_key,
        measure_source,
        measure_control,
        period_start,
        period_end,
        value_text,
        value_number,
        unit_count,
        skipped_count,
        is_conflict
    from owners
    union all
    select
        spine_key,
        measure_source,
        measure_control,
        period_start,
        period_end,
        value_text,
        value_number,
        unit_count,
        skipped_count,
        is_conflict
    from chow
),

any_source as (
    select
        ccn,
        'hhs_capacity' as measure_source
    from hhs_weeks
    union
    select
        ccn,
        'onc' as measure_source
    from onc_periods
    union
    select
        ccn,
        'ownership' as measure_source
    from enrollment_ccns
    union
    select
        ccn,
        'ownership' as measure_source
    from chow_events
)

select
    spine.spine_key,
    spine.ccn,
    spine.window_year,
    spine.window_start,
    controls.measure_source,
    controls.measure_control,
    controls.review_decision,
    chosen.period_start,
    chosen.period_end,
    chosen.unit_count,
    chosen.skipped_count,
    spine.is_primary_population,
    spine.is_sensitivity_population,
    spine.is_connecticut,
    spine.is_maryland,
    case when not chosen.is_conflict then chosen.value_text end as value_text,
    case when not chosen.is_conflict then chosen.value_number end as value_number,
    case when chosen.period_end is not null then datediff('month', chosen.period_end, spine.window_start) end as age_months,
    case
        when chosen.is_conflict then 'held_in_staging'
        when chosen.spine_key is not null and (chosen.value_text is not null or chosen.value_number is not null) then 'aligned'
        when chosen.spine_key is not null and chosen.skipped_count > 0 then 'held_in_staging'
        -- A period with rows but no value for this control (for example a field the source does not publish).
        when chosen.spine_key is not null then 'not_in_source'
        when any_source.ccn is not null then 'no_period_before_start'
        else 'not_in_source'
    end as alignment_status,
    spine.spine_key || ':' || controls.measure_source || ':' || controls.measure_control as alignment_key
from spine
cross join controls
left join chosen
    on
        spine.spine_key = chosen.spine_key
        and controls.measure_source = chosen.measure_source
        and controls.measure_control = chosen.measure_control
left join any_source
    on
        spine.ccn = any_source.ccn
        and controls.measure_source = any_source.measure_source
