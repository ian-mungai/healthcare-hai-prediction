-- One row per hospital (CCN) and calendar-year HAI window, as of the window start: the POS
-- snapshot that ends in the 12 months before the window, the CMI of the fiscal year that ends before it and the model
-- population flags (failure modes 306 to 316). A missing state, ZIP, county, planning region or hospital classification is
-- filled only from a source whose period ends strictly before the window start (the as-of rule), and each filled field names
-- its source and that source's period end. A source that ends on or after the start, a later snapshot or a fill whose
-- choice depends on a later period is never used.
-- A table in the build's DuckDB file only, never published (the feature phase keeps the other models as views).
{{ config(materialized='table') }}

with

windows as (
    -- Calendar-year windows only; rolling and 18-month windows are not spine years [306] [307].
    select
        entity_id as ccn,
        window_start,
        year(window_start) as window_year,
        count(distinct measure_id) as hai_measure_count
    from {{ ref('fct_hai_hospital_window') }}
    where
        month(window_start) = 1
        and day(window_start) = 1
        and window_end = make_date(year(window_start), 12, 31)
    group by
        entity_id,
        window_start
),

window_starts as (
    select distinct window_start
    from windows
),

snapshot_periods as (
    select distinct period_end
    from {{ ref('fct_pos_snapshot') }}
),

window_snapshots as (
    -- The latest snapshot that ends in the 12 months before the window; never one from inside or after it [308] [309].
    select
        window_starts.window_start,
        max(snapshot_periods.period_end) as pos_period_end
    from window_starts
    inner join snapshot_periods
        on
            window_starts.window_start > snapshot_periods.period_end
            and (window_starts.window_start - interval 1 year)::date <= snapshot_periods.period_end
    group by window_starts.window_start
),

pos as (
    select
        ccn,
        period_end,
        state_code,
        county_fips,
        zip_code,
        provider_subtype_code,
        control_type_code,
        bed_count,
        certified_bed_count,
        is_active
    from {{ ref('fct_pos_snapshot') }}
),

earlier as (
    -- The same CCN's latest snapshot that ends before the window start, within 24 months [613] [701].
    select
        windows.ccn,
        windows.window_start,
        pos.period_end,
        pos.state_code,
        pos.county_fips,
        pos.zip_code,
        pos.provider_subtype_code,
        pos.control_type_code
    from windows
    inner join pos
        on
            windows.ccn = pos.ccn
            and windows.window_start > pos.period_end
            and pos.period_end >= (windows.window_start - interval 24 month)::date
    qualify row_number() over (partition by windows.ccn, windows.window_start order by pos.period_end desc, pos.county_fips asc nulls last) = 1
),

enrollment as (
    -- The CCN's enrollment row with the latest period that ends before the window start [615].
    select
        windows.ccn,
        windows.window_start,
        enrollment_rows.period_end,
        enrollment_rows.state,
        left(enrollment_rows.zip_code, 5) as zip_code
    from windows
    inner join {{ ref('fct_enrollment_release') }} as enrollment_rows
        on
            windows.ccn = enrollment_rows.ccn
            and windows.window_start > enrollment_rows.period_end
    qualify row_number() over (
        partition by windows.ccn, windows.window_start
        order by enrollment_rows.period_end desc, enrollment_rows.enrollment_key asc
    ) = 1
),

hud as (
    select
        zip_code,
        county_fips,
        usps_state,
        quarter_start_date,
        quarter_end_date,
        count(*) over (partition by zip_code, quarter_start_date, county_fips like '091%') as counties_in_zip
    from {{ ref('bridge_zip_county') }}
),

cmi as (
    select
        ccn,
        data_fiscal_year,
        rule_fiscal_year,
        rule_stage,
        cmi,
        cases
    from {{ ref('int_cmi_hospital_data_years') }}
),

cmi_holds as (
    select
        ccn,
        fiscal_year
    from {{ ref('int_cmi_holds') }}
    where year_basis = 'data'
),

located as (
    -- Own snapshot first, then the CCN's latest earlier snapshot, then the latest enrollment row, all ending before the window
    -- start [612] [615]. Each field names its source and that source's period end; the classification comes from the own
    -- snapshot, else the earlier one [614].
    select
        windows.ccn,
        windows.window_year,
        windows.window_start,
        windows.hai_measure_count,
        pos.period_end as pos_period_end,
        pos.bed_count,
        pos.certified_bed_count,
        pos.is_active,
        pos.ccn is not null as has_pos_snapshot,
        coalesce(pos.state_code, earlier.state_code, enrollment.state) as state_code,
        case
            when pos.state_code is not null then 'pos_own'
            when earlier.state_code is not null then 'pos_earlier'
            when enrollment.state is not null then 'enrollment'
        end as state_source,
        case
            when pos.state_code is not null then pos.period_end
            when earlier.state_code is not null then earlier.period_end
            when enrollment.state is not null then enrollment.period_end
        end as state_source_period_end,
        coalesce(pos.county_fips, earlier.county_fips) as snapshot_county_fips,
        case
            when pos.county_fips is not null then 'pos_own'
            when earlier.county_fips is not null then 'pos_earlier'
        end as snapshot_county_source,
        case
            when pos.county_fips is not null then pos.period_end
            when earlier.county_fips is not null then earlier.period_end
        end as snapshot_county_period_end,
        coalesce(left(pos.zip_code, 5), left(earlier.zip_code, 5), enrollment.zip_code) as zip_code,
        case
            when pos.zip_code is not null then 'pos_own'
            when earlier.zip_code is not null then 'pos_earlier'
            when enrollment.zip_code is not null then 'enrollment'
        end as zip_source,
        case
            when pos.zip_code is not null then pos.period_end
            when earlier.zip_code is not null then earlier.period_end
            when enrollment.zip_code is not null then enrollment.period_end
        end as zip_source_period_end,
        case
            when pos.ccn is not null then pos.provider_subtype_code
            when earlier.ccn is not null then earlier.provider_subtype_code
        end as provider_subtype_code,
        case
            when pos.ccn is not null then pos.control_type_code
            when earlier.ccn is not null then earlier.control_type_code
        end as control_type_code,
        case
            when pos.ccn is not null then 'pos_own'
            when earlier.ccn is not null then 'pos_earlier'
        end as classification_source,
        case
            when pos.ccn is not null then pos.period_end
            when earlier.ccn is not null then earlier.period_end
        end as classification_source_period_end
    from windows
    left join window_snapshots on windows.window_start = window_snapshots.window_start
    left join pos
        on
            windows.ccn = pos.ccn
            and window_snapshots.pos_period_end = pos.period_end
    left join earlier on windows.ccn = earlier.ccn and windows.window_start = earlier.window_start
    left join enrollment on windows.ccn = enrollment.ccn and windows.window_start = enrollment.window_start
),

zip_counties as (
    -- The latest HUD quarter that ends before the window start; the ZIP's state must be the hospital's. A ZIP split across
    -- counties in that quarter gives none: a majority share is not an exact derivation [616] [698].
    select
        located.ccn,
        located.window_start,
        hud.county_fips,
        hud.quarter_end_date,
        hud.counties_in_zip
    from located
    inner join hud
        on
            located.zip_code = hud.zip_code
            and located.state_code = hud.usps_state
            and hud.county_fips not like '091%'
            and located.window_start > hud.quarter_end_date
    where located.snapshot_county_fips is null
    qualify row_number() over (
        partition by located.ccn, located.window_start
        order by hud.quarter_end_date desc, hud.county_fips asc
    ) = 1
),

regions as (
    -- Connecticut's planning region for the hospital ZIP, from the latest HUD quarter that ends before the window start and
    -- publishes regions; a ZIP split across regions gives none [631] [698].
    select
        located.ccn,
        located.window_start,
        hud.county_fips as planning_region_fips,
        hud.quarter_end_date,
        hud.counties_in_zip
    from located
    inner join hud
        on
            located.zip_code = hud.zip_code
            and hud.usps_state = 'CT'
            and hud.county_fips like '091%'
            and located.window_start > hud.quarter_end_date
    where located.state_code = 'CT'
    qualify row_number() over (
        partition by located.ccn, located.window_start
        order by hud.quarter_end_date desc, hud.county_fips asc
    ) = 1
),

joined as (
    -- The CMI is the data year that ends before the window [310]; a held CMI stays empty [315].
    select
        located.ccn,
        located.window_year,
        located.window_start,
        located.hai_measure_count,
        located.pos_period_end,
        located.state_code,
        located.state_source,
        located.state_source_period_end,
        located.zip_code,
        located.zip_source,
        located.zip_source_period_end,
        coalesce(located.snapshot_county_fips, zip_counties.county_fips) as county_fips,
        case
            when located.snapshot_county_fips is not null then located.snapshot_county_source
            when zip_counties.county_fips is not null then 'hud_quarter'
        end as county_source,
        case
            when located.snapshot_county_fips is not null then located.snapshot_county_period_end
            when zip_counties.county_fips is not null then zip_counties.quarter_end_date
        end as county_source_period_end,
        regions.planning_region_fips,
        case when regions.planning_region_fips is not null then 'hud_quarter' end as planning_region_source,
        regions.quarter_end_date as planning_region_source_period_end,
        located.provider_subtype_code,
        located.control_type_code,
        located.classification_source,
        located.classification_source_period_end,
        located.bed_count,
        located.certified_bed_count,
        located.is_active,
        cmi.cmi,
        cmi.data_fiscal_year as cmi_data_fiscal_year,
        cmi.rule_fiscal_year as cmi_rule_fiscal_year,
        cmi.rule_stage as cmi_rule_stage,
        cmi.cases as cmi_cases,
        located.has_pos_snapshot,
        cmi.ccn is not null as has_cmi,
        cmi_holds.ccn is not null as is_cmi_held,
        substr(located.ccn, 3, 2) = '13' as is_critical_access,
        right(located.ccn, 1) = 'F' as is_veterans_affairs,
        -- Flags follow the filled state [617].
        coalesce(located.state_code in {{ us_states_and_dc() }}, false) as is_state_or_dc,
        coalesce(located.state_code = 'CT', false) as is_connecticut,
        coalesce(located.state_code = 'MD', false) as is_maryland
    from located
    left join zip_counties
        on
            located.ccn = zip_counties.ccn
            and located.window_start = zip_counties.window_start
            and zip_counties.counties_in_zip = 1
    left join regions
        on
            located.ccn = regions.ccn
            and located.window_start = regions.window_start
            and regions.counties_in_zip = 1
    left join cmi
        on
            located.ccn = cmi.ccn
            and located.window_year - 1 = cmi.data_fiscal_year
    left join cmi_holds
        on
            located.ccn = cmi_holds.ccn
            and located.window_year - 1 = cmi_holds.fiscal_year
)

-- Primary: IPPS hospitals with a CMI in the 50 states and DC, without Veterans Health Administration or critical access
-- hospitals and without Maryland, which is paid under its own all-payer model. The sensitivity
-- run adds critical access and Maryland hospitals [313] [317]. Connecticut stays in both [316].
select
    *,
    ccn || ':' || window_year as spine_key,
    has_cmi and is_state_or_dc and not is_veterans_affairs and not is_critical_access and not is_maryland as is_primary_population,
    (has_cmi or is_critical_access) and is_state_or_dc and not is_veterans_affairs as is_sensitivity_population
from joined
