-- One row per spine hospital-window and county context control-field from ACS, SVI, SAIPE, SAHIE and BLS (the latest
-- vintage, edition or year that ends before the HAI window starts), HPSA and MUA designations in force at the start
-- (rebuilt from the designation and withdrawal dates) and RUCA codes for the hospital ZIP; the HUD and service-area
-- linkage is in int_spine_linkage. Counties come from the filled spine; Connecticut matches on its old county or its
-- planning region (alignment step AL4b, failure modes 604 to 611 and 634 to 637, plans/alignment_20261008/failure_modes_al4.md).
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

spine_geos as (
    -- Each window's county and, for Connecticut, its planning region, so the county joins stay equality joins [632].
    select
        spine_key,
        window_start,
        county_fips as geo_fips
    from spine
    where county_fips is not null
    union
    select
        spine_key,
        window_start,
        planning_region_fips as geo_fips
    from spine
    where planning_region_fips is not null
),

value_controls as (
    -- One control-field per published component; ACS and SVI stay separate sources of one control [634].
    select
        measure_control,
        review_decision,
        source as measure_source,
        unnest(string_split(components, ' ')) as field
    from {{ ref('acs_svi_measures') }}
    where source in ('acs', 'svi') and nullif(components, '') is not null
    union all
    select
        income.measure_control,
        income.review_decision,
        case income.source_model
            when 'int_saipe_county_estimates' then 'saipe'
            when 'int_sahie_county_rows' then 'sahie'
            else 'bls'
        end as measure_source,
        unnest(
            case income.source_model
                when 'int_bls_county_series' then ['unemployment_rate', 'unemployed', 'employed', 'labor_force']
                else list_filter(
                    string_split(income.fields, ' '),
                    x -> x not in ('estimate_year', 'county_fips', 'agecat', 'racecat', 'sexcat', 'iprcat', 'is_all_groups')
                )
            end
        ) as field
    from {{ ref('income_labor_measures') }} as income
    where nullif(income.fields, '') is not null
),

saipe_long as (
    unpivot (
        select
            county_fips,
            estimate_year,
            poverty_all_pct,
            poverty_all_pct_lb90,
            poverty_all_pct_ub90,
            poverty_all_count,
            median_household_income,
            median_household_income_lb90,
            median_household_income_ub90
        from {{ ref('int_saipe_county_estimates') }}
    )
    on poverty_all_pct, poverty_all_pct_lb90, poverty_all_pct_ub90, poverty_all_count, median_household_income,
    median_household_income_lb90, median_household_income_ub90
    into name field value value_number
),

sahie_latest as (
    -- All-group rows only [635]; the latest file of each estimate year (owner decision 5).
    select
        county_fips,
        estimate_year,
        nipr,
        nui,
        pctui,
        pctui_moe
    from {{ ref('int_sahie_county_rows') }}
    where is_all_groups
    qualify rank() over (partition by county_fips, estimate_year order by file_year desc) = 1
),

sahie_long as (
    unpivot sahie_latest
    on nipr, nui, pctui, pctui_moe
    into name field value value_number
),

periods as (
    select
        county_fips,
        concept_id as field,
        value_published as value_text,
        value_number,
        'acs' as measure_source,
        make_date(vintage::integer, 12, 31) as period_end
    from {{ ref('int_acs_county_values') }}
    union all
    select
        county_fips,
        field,
        value_published as value_text,
        value_number,
        'svi' as measure_source,
        make_date(edition::integer, 12, 31) as period_end
    from {{ ref('int_svi_county_values') }}
    union all
    select
        county_fips,
        field,
        value_number::varchar as value_text,
        value_number,
        'saipe' as measure_source,
        make_date(estimate_year, 12, 31) as period_end
    from saipe_long
    union all
    select
        county_fips,
        field,
        value_number::varchar as value_text,
        value_number,
        'sahie' as measure_source,
        make_date(estimate_year, 12, 31) as period_end
    from sahie_long
    union all
    -- Annual averages only [635].
    select
        county_fips,
        measure as field,
        value_published as value_text,
        value_number,
        'bls' as measure_source,
        make_date(data_year, 12, 31) as period_end
    from {{ ref('int_bls_county_series') }}
    where is_annual_average
),

control_periods as (
    select
        periods.county_fips,
        periods.value_text,
        periods.value_number,
        periods.period_end,
        value_controls.measure_control,
        value_controls.measure_source,
        value_controls.field
    from periods
    inner join value_controls
        on
            periods.measure_source = value_controls.measure_source
            and periods.field = value_controls.field
),

value_candidates as (
    -- The latest period that ends before the HAI window starts [604] [634]; two values for one period are held [632].
    select
        spine.spine_key,
        control_periods.measure_control,
        control_periods.measure_source,
        control_periods.field,
        control_periods.period_end,
        count(distinct coalesce(control_periods.value_text, control_periods.value_number::varchar)) > 1 as is_conflict,
        max(control_periods.value_text) as value_text,
        max(control_periods.value_number) as value_number,
        row_number() over (
            partition by spine.spine_key, control_periods.measure_control, control_periods.measure_source, control_periods.field
            order by control_periods.period_end desc
        ) as recency
    from spine_geos as spine
    inner join control_periods
        on
            spine.geo_fips = control_periods.county_fips
            and spine.window_start > control_periods.period_end
    group by
        spine.spine_key,
        control_periods.measure_control,
        control_periods.measure_source,
        control_periods.field,
        control_periods.period_end
),

value_any_period as (
    select distinct
        spine.spine_key,
        control_periods.measure_control,
        control_periods.measure_source,
        control_periods.field
    from spine_geos as spine
    inner join control_periods on spine.geo_fips = control_periods.county_fips
),

value_rows as (
    select
        spine.spine_key,
        value_controls.measure_source,
        value_controls.measure_control,
        value_controls.field,
        value_controls.review_decision,
        chosen.period_end,
        case when not chosen.is_conflict then chosen.value_text end as value_text,
        case when not chosen.is_conflict then chosen.value_number end as value_number,
        null::varchar as value_note,
        case
            when chosen.is_conflict then 'held_in_staging'
            when chosen.spine_key is not null then 'aligned'
            when value_any_period.spine_key is not null then 'no_period_before_start'
            else 'not_in_source'
        end as alignment_status
    from spine
    cross join value_controls
    left join value_candidates as chosen
        on
            spine.spine_key = chosen.spine_key
            and value_controls.measure_control = chosen.measure_control
            and value_controls.measure_source = chosen.measure_source
            and value_controls.field = chosen.field
            and chosen.recency = 1
    left join value_any_period
        on
            spine.spine_key = value_any_period.spine_key
            and value_controls.measure_control = value_any_period.measure_control
            and value_controls.measure_source = value_any_period.measure_source
            and value_controls.field = value_any_period.field
),

designations as (
    -- HPSA and MUA components with their dates; a withdrawal without a date and the 1970 placeholder are held [636].
    select
        'hpsa' as measure_source,
        county_fips,
        hpsa_id as designation_id,
        designation_date,
        withdrawn_date as withdrawal_date,
        hpsa_score::double as score,
        hold_reason is null
        and not (hpsa_status = 'Withdrawn' and withdrawn_date is null)
        and coalesce(designation_date <> date '1970-01-01', true) as is_dated
    from {{ ref('int_hpsa_components') }}
    where county_fips is not null and (hold_reason is null or hold_reason <> 'exact_repeat')
    union all
    select
        'mua' as measure_source,
        county_fips,
        mua_id as designation_id,
        designation_date,
        withdrawal_date,
        imu_score as score,
        not(mua_status = 'Withdrawn' and withdrawal_date is null) as is_dated
    from {{ ref('int_mua_components') }}
    where county_fips is not null and (hold_reason is null or hold_reason <> 'exact_repeat')
),

designation_windows as (
    -- In force at the start: designated before it and not withdrawn on or before it [636].
    select
        spine.spine_key,
        designations.measure_source,
        count(distinct designations.designation_id) filter (
            where
            designations.is_dated
            and designations.designation_date < spine.window_start
            and (designations.withdrawal_date is null or designations.withdrawal_date > spine.window_start)
        ) as designations_in_force,
        max(designations.score) filter (
            where
            designations.is_dated
            and designations.designation_date < spine.window_start
            and (designations.withdrawal_date is null or designations.withdrawal_date > spine.window_start)
        ) as highest_score,
        bool_or(not designations.is_dated and coalesce(designations.designation_date, date '1970-01-01') < spine.window_start)
            as has_held
    from spine_geos as spine_geo
    inner join spine on spine_geo.spine_key = spine.spine_key
    inner join designations on spine_geo.geo_fips = designations.county_fips
    group by
        spine.spine_key,
        designations.measure_source
),

designation_controls as (
    -- C249 counts HPSA designations in force, C250 takes their highest score; C251 carries both for MUA.
    select
        measure_control,
        review_decision,
        case source_model when 'int_hpsa_components' then 'hpsa' else 'mua' end as measure_source,
        unnest(
            case measure_control
                when 'C249' then ['designations_in_force']
                when 'C250' then ['highest_score']
                else ['designations_in_force', 'highest_score']
            end
        ) as field
    from {{ ref('shortage_measures') }}
    where source_model in ('int_hpsa_components', 'int_mua_components')
),

designation_rows as (
    select
        spine.spine_key,
        designation_controls.measure_source,
        designation_controls.measure_control,
        designation_controls.field,
        designation_controls.review_decision,
        (spine.window_start - interval 1 day)::date as period_end,
        case
            when designation_windows.has_held then null
            when designation_controls.field = 'designations_in_force' then coalesce(designation_windows.designations_in_force, 0)::varchar
            else designation_windows.highest_score::varchar
        end as value_text,
        case
            when designation_windows.has_held then null
            when designation_controls.field = 'designations_in_force' then coalesce(designation_windows.designations_in_force, 0)
            else designation_windows.highest_score
        end::double as value_number,
        case
            when designation_windows.has_held then null
            when designation_controls.field = 'highest_score' and coalesce(designation_windows.designations_in_force, 0) = 0
                then 'none_in_force'
            when designation_controls.field = 'highest_score' then 'score_as_published_at_capture'
        end as value_note,
        case
            when designation_windows.has_held then 'held_in_staging'
            when spine.county_fips is null and spine.planning_region_fips is null then 'not_in_source'
            else 'aligned'
        end as alignment_status
    from spine
    cross join designation_controls
    left join designation_windows
        on
            spine.spine_key = designation_windows.spine_key
            and designation_controls.measure_source = designation_windows.measure_source
),

ruca_controls as (
    select
        measure_control,
        review_decision,
        unnest(list_filter(string_split(fields, ' '), x -> x in ('primary_ruca', 'secondary_ruca'))) as field
    from {{ ref('geography_measures') }}
    where source_model = 'int_ruca_codes'
),

ruca_candidates as (
    -- The hospital ZIP's code from the latest vintage before the window [637].
    select
        spine.spine_key,
        ruca.primary_ruca,
        ruca.secondary_ruca,
        make_date(ruca.vintage::integer, 12, 31) as period_end
    from spine
    inner join {{ ref('int_ruca_codes') }} as ruca
        on
            ruca.geography_type = 'zip'
            and spine.zip_code = ruca.geography_id
            and spine.window_start > make_date(ruca.vintage::integer, 12, 31)
    qualify row_number() over (partition by spine.spine_key order by ruca.vintage desc, ruca.ruca_row_key asc) = 1
),

ruca_zips as (
    select distinct geography_id as zip_code
    from {{ ref('int_ruca_codes') }}
    where geography_type = 'zip'
),

ruca_rows as (
    select
        spine.spine_key,
        'ruca' as measure_source,
        ruca_controls.measure_control,
        ruca_controls.field,
        ruca_controls.review_decision,
        ruca_candidates.period_end,
        case ruca_controls.field when 'primary_ruca' then ruca_candidates.primary_ruca else ruca_candidates.secondary_ruca end::varchar
            as value_text,
        case ruca_controls.field when 'primary_ruca' then ruca_candidates.primary_ruca else ruca_candidates.secondary_ruca end::double
            as value_number,
        'zip_code_approximation' as value_note,
        case
            when ruca_candidates.spine_key is not null then 'aligned'
            when ruca_zips.zip_code is not null then 'no_period_before_start'
            else 'not_in_source'
        end as alignment_status
    from spine
    cross join ruca_controls
    left join ruca_candidates on spine.spine_key = ruca_candidates.spine_key
    left join ruca_zips on spine.zip_code = ruca_zips.zip_code
),

all_rows as (
    select * from value_rows
    union all
    select * from designation_rows
    union all
    select * from ruca_rows
)

select
    spine.spine_key,
    spine.ccn,
    spine.window_year,
    spine.window_start,
    spine.county_fips,
    spine.planning_region_fips,
    all_rows.measure_source,
    all_rows.measure_control,
    all_rows.field,
    all_rows.review_decision,
    all_rows.period_end,
    all_rows.value_text,
    all_rows.value_number,
    all_rows.value_note,
    all_rows.alignment_status,
    spine.is_primary_population,
    spine.is_sensitivity_population,
    spine.is_connecticut,
    spine.is_maryland,
    case when all_rows.period_end is not null then datediff('month', all_rows.period_end, spine.window_start) end as age_months,
    spine.spine_key || ':' || all_rows.measure_source || ':' || all_rows.measure_control || ':' || all_rows.field as alignment_key
from all_rows
inner join spine on all_rows.spine_key = spine.spine_key
