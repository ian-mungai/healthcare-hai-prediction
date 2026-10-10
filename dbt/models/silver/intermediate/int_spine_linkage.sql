-- One row per spine hospital-window and linkage field: the share of the hospital ZIP's residential addresses in its county
-- or planning region (HUD, the latest quarter that ends before the start) and the hospital's service-area ZIPs and cases
-- (the latest data year before the window, suppressed rows left out). Linkage only, never a predictor (alignment step
-- AL4b, failure mode 638, plans/alignment_20261008/failure_modes_al4.md).
{{ config(materialized='table') }}

with

spine as (
    select
        ccn,
        window_year,
        window_start,
        spine_key,
        county_fips,
        planning_region_fips,
        zip_code,
        is_primary_population,
        is_sensitivity_population,
        is_connecticut,
        is_maryland
    from {{ ref('int_hospital_spine') }}
),

hud_shares as (
    -- The share of the hospital ZIP's residential addresses in its county or region, the latest quarter before the start.
    select
        spine.spine_key,
        hud.quarter_end_date as period_end,
        hud.res_ratio
    from spine
    inner join {{ ref('int_hud_zip_county_quarters') }} as hud
        on
            spine.zip_code = hud.zip_code
            and (spine.county_fips = hud.county_fips or spine.planning_region_fips = hud.county_fips)
            and spine.window_start > hud.quarter_end_date
    qualify row_number() over (partition by spine.spine_key order by hud.quarter_end_date desc, hud.county_fips asc) = 1
),

service_area_rows as (
    -- Published, unsuppressed service-area rows of a valid CCN.
    select
        ccn_published as ccn,
        data_year::integer as data_year,
        period_end,
        zip_code,
        total_cases
    from {{ ref('int_hsa_zip_cases') }}
    where
        is_ccn_shape_valid
        and not is_zip_suppressed
        and not is_zip_missing
        and not is_cases_suppressed
),

service_area_years as (
    -- The latest data year that ends before the window starts.
    select
        spine.spine_key,
        spine.ccn,
        max(service_area_rows.data_year) as data_year
    from spine
    inner join service_area_rows
        on
            spine.ccn = service_area_rows.ccn
            and spine.window_year > service_area_rows.data_year
    group by
        spine.spine_key,
        spine.ccn
),

service_areas as (
    -- The hospital's service-area ZIPs and cases in that year.
    select
        service_area_years.spine_key,
        max(service_area_rows.period_end) as period_end,
        count(distinct service_area_rows.zip_code)::double as service_area_zips,
        sum(service_area_rows.total_cases)::double as service_area_cases
    from service_area_years
    inner join service_area_rows
        on
            service_area_years.ccn = service_area_rows.ccn
            and service_area_years.data_year = service_area_rows.data_year
    group by service_area_years.spine_key
),

linkage_presence as (
    -- The hospital's ZIP in HUD for its county or region, or the hospital in the service-area file, in any period.
    select distinct
        spine.spine_key,
        'hud' as measure_source
    from spine
    inner join {{ ref('int_hud_zip_county_quarters') }} as hud
        on
            spine.zip_code = hud.zip_code
            and (spine.county_fips = hud.county_fips or spine.planning_region_fips = hud.county_fips)
    union all
    select distinct
        spine.spine_key,
        'hsa' as measure_source
    from spine
    inner join {{ ref('int_hsa_zip_cases') }} as cases on spine.ccn = cases.ccn_published
),

linkage_controls as (
    select
        'hud' as measure_source,
        'L002' as measure_control,
        'zip_residential_share' as field
    union all
    select
        'hsa' as measure_source,
        'L001' as measure_control,
        unnest(['service_area_zips', 'service_area_cases']) as field
),

linkage_rows as (
    -- Linkage only, never a predictor [638].
    select
        spine.spine_key,
        linkage_controls.measure_source,
        linkage_controls.measure_control,
        linkage_controls.field,
        'linkage_only' as review_decision,
        coalesce(hud_shares.period_end, service_areas.period_end) as period_end,
        case linkage_controls.field
            when 'zip_residential_share' then hud_shares.res_ratio
            when 'service_area_zips' then service_areas.service_area_zips
            else service_areas.service_area_cases
        end::varchar as value_text,
        case linkage_controls.field
            when 'zip_residential_share' then hud_shares.res_ratio
            when 'service_area_zips' then service_areas.service_area_zips
            else service_areas.service_area_cases
        end as value_number,
        case linkage_controls.measure_source when 'hsa' then 'published_counts_only' end as value_note,
        case
            when coalesce(hud_shares.spine_key, service_areas.spine_key) is not null then 'aligned'
            when linkage_presence.spine_key is not null then 'no_period_before_start'
            else 'not_in_source'
        end as alignment_status
    from spine
    cross join linkage_controls
    left join hud_shares
        on
            spine.spine_key = hud_shares.spine_key
            and linkage_controls.measure_source = 'hud'
    left join service_areas
        on
            spine.spine_key = service_areas.spine_key
            and linkage_controls.measure_source = 'hsa'
    left join linkage_presence
        on
            spine.spine_key = linkage_presence.spine_key
            and linkage_controls.measure_source = linkage_presence.measure_source
)

select
    spine.spine_key,
    spine.ccn,
    spine.window_year,
    spine.window_start,
    spine.county_fips,
    spine.planning_region_fips,
    linkage_rows.measure_source,
    linkage_rows.measure_control,
    linkage_rows.field,
    linkage_rows.review_decision,
    linkage_rows.period_end,
    linkage_rows.value_text,
    linkage_rows.value_number,
    linkage_rows.value_note,
    linkage_rows.alignment_status,
    spine.is_primary_population,
    spine.is_sensitivity_population,
    spine.is_connecticut,
    spine.is_maryland,
    case when linkage_rows.period_end is not null then datediff('month', linkage_rows.period_end, spine.window_start) end as age_months,
    spine.spine_key || ':' || linkage_rows.measure_source || ':' || linkage_rows.measure_control || ':' || linkage_rows.field as alignment_key
from linkage_rows
inner join spine on linkage_rows.spine_key = spine.spine_key
