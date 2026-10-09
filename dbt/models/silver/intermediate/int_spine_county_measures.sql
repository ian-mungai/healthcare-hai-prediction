-- One row per spine hospital-window and county control-field from PLACES, Medicare Geographic Variation, CDC WONDER, CMS
-- Mapping Medicare Disparities and the Rural-Urban Continuum Codes, through the hospital's POS county: the latest data
-- year (or RUCC vintage) that ends before the HAI window starts. A PLACES data year published twice takes the latest
-- release; only all-state PLACES measure-years count; WONDER 2018 to 2020 come from the single-race database. Connecticut
-- rows are kept and flagged (alignment step AL4a, failure modes 604 to 611, plans/alignment_20261008/failure_modes_al4.md).
{{ config(materialized='table') }}

with

spine as (
    select
        ccn,
        window_year,
        window_start,
        spine_key,
        county_fips,
        is_primary_population,
        is_sensitivity_population,
        is_connecticut,
        is_maryland
    from {{ ref('int_hospital_spine') }}
),

health_controls as (
    select
        measure_control,
        source_model,
        review_decision,
        unnest(string_split(components, ' ')) as component
    from {{ ref('county_health_measures') }}
),

value_types as (
    select unnest(['AgeAdjPrv', 'CrdPrv']) as value_type
),

controls as (
    select
        health_controls.measure_control,
        health_controls.review_decision,
        'places' as measure_source,
        health_controls.component || ':' || value_types.value_type as field
    from health_controls
    cross join value_types
    where health_controls.source_model = 'int_places_county_values'
    union all
    select
        measure_control,
        review_decision,
        'geographic_variation' as measure_source,
        component as field
    from health_controls
    where source_model = 'int_gv_county_values'
    union all
    select
        measure_control,
        review_decision,
        'wonder' as measure_source,
        component as field
    from health_controls
    where source_model = 'int_wonder_county_deaths'
    union all
    select
        measure_control,
        'retain_conditional' as review_decision,
        'mmd' as measure_source,
        'prevalence' as field
    from {{ ref('mmd_conditions') }}
    where geography_level = 'county'
    union all
    select
        measure_control,
        review_decision,
        'rucc' as measure_source,
        'rucc_code' as field
    from {{ ref('geography_measures') }}
    where source_model = 'int_rucc_county_codes'
),

places as (
    -- All-state measure-years only [607]; the latest release of each data year (owner decision 5).
    select
        county_fips,
        data_year,
        data_value_published as value_text,
        data_value::double as value_number,
        measureid || ':' || datavaluetypeid as field,
        row_number() over (
            partition by county_fips, measureid, datavaluetypeid, data_year
            order by edition desc
        ) as release_rank
    from {{ ref('int_places_county_values') }}
    where is_all_states
),

wonder_long as (
    -- WONDER 2018 to 2020 from the single-race database, which continues to 2024; earlier years from the bridged one.
    unpivot (
        select
            county_fips,
            data_year,
            deaths::varchar as deaths,
            population::varchar as population,
            crude_rate::varchar as crude_rate,
            crude_rate_lower_95::varchar as crude_rate_lower_95,
            crude_rate_upper_95::varchar as crude_rate_upper_95,
            crude_rate_standard_error::varchar as crude_rate_standard_error,
            deaths_token,
            crude_rate_token
        from {{ ref('int_wonder_county_deaths') }}
        where
            hold_reason is null
            and (
                data_year < 2018 and wonder_database like '%1999-2020%'
                or data_year >= 2018 and wonder_database like '%Single Race%'
            )
    )
    on deaths, population, crude_rate, crude_rate_lower_95, crude_rate_upper_95, crude_rate_standard_error, deaths_token, crude_rate_token
    into name field value value_text
),

periods as (
    select
        county_fips,
        field,
        value_text,
        value_number,
        'places' as measure_source,
        make_date(data_year, 12, 31) as period_end
    from places
    where release_rank = 1
    union all
    select
        county_fips,
        field,
        value_published as value_text,
        value_number,
        'geographic_variation' as measure_source,
        make_date(data_year, 12, 31) as period_end
    from {{ ref('int_gv_county_values') }}
    union all
    select
        county_fips,
        field,
        value_text,
        try_cast(value_text as double) as value_number,
        'wonder' as measure_source,
        make_date(data_year, 12, 31) as period_end
    from wonder_long
    union all
    select
        county_fips,
        measure_control as field,
        value_published as value_text,
        value_number,
        'mmd' as measure_source,
        make_date(data_year, 12, 31) as period_end
    from {{ ref('int_mmd_prevalence') }}
    where geography_level = 'county'
    union all
    select
        county_fips,
        'rucc_code' as field,
        rucc_code as value_text,
        null::double as value_number,
        'rucc' as measure_source,
        make_date(vintage::integer, 12, 31) as period_end
    from {{ ref('int_rucc_county_codes') }}
),

control_periods as (
    -- MMD rows name their own control; the other sources join their control through the field.
    select
        periods.county_fips,
        periods.value_text,
        periods.value_number,
        periods.period_end,
        controls.measure_control,
        controls.measure_source,
        controls.field
    from periods
    inner join controls
        on
            periods.measure_source = controls.measure_source
            and (
                periods.measure_source = 'mmd' and periods.field = controls.measure_control
                or periods.measure_source <> 'mmd' and periods.field = controls.field
            )
),

candidates as (
    -- The latest period that ends before the HAI window starts [604]; disagreeing values for one period are held.
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
            partition by spine.spine_key, control_periods.measure_control, control_periods.field
            order by control_periods.period_end desc
        ) as recency
    from spine
    inner join control_periods
        on
            spine.county_fips = control_periods.county_fips
            and spine.window_start > control_periods.period_end
    group by
        spine.spine_key,
        control_periods.measure_control,
        control_periods.measure_source,
        control_periods.field,
        control_periods.period_end
),

chosen as (
    select * from candidates
    where recency = 1
),

any_period as (
    select distinct
        county_fips,
        measure_control,
        field
    from control_periods
)

select
    spine.spine_key,
    spine.ccn,
    spine.window_year,
    spine.window_start,
    spine.county_fips,
    controls.measure_source,
    controls.measure_control,
    controls.field,
    controls.review_decision,
    chosen.period_end,
    spine.is_primary_population,
    spine.is_sensitivity_population,
    spine.is_connecticut,
    spine.is_maryland,
    case when not chosen.is_conflict then chosen.value_text end as value_text,
    case when not chosen.is_conflict then chosen.value_number end as value_number,
    case when chosen.period_end is not null then datediff('month', chosen.period_end, spine.window_start) end as age_months,
    -- Connecticut stays out of the primary model's county joins until a relationship file is captured [606].
    not spine.is_connecticut and spine.county_fips is not null as is_primary_county_join,
    case
        when chosen.is_conflict then 'held_in_staging'
        when chosen.spine_key is not null then 'aligned'
        when any_period.county_fips is not null then 'no_period_before_start'
        else 'not_in_source'
    end as alignment_status,
    spine.spine_key || ':' || controls.measure_source || ':' || controls.measure_control || ':' || controls.field as alignment_key
from spine
cross join controls
left join chosen
    on
        spine.spine_key = chosen.spine_key
        and controls.measure_control = chosen.measure_control
        and controls.field = chosen.field
left join any_period
    on
        spine.county_fips = any_period.county_fips
        and controls.measure_control = any_period.measure_control
        and controls.field = any_period.field
