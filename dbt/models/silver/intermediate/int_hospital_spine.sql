-- One row per hospital (CCN) and calendar-year HAI window, as of the window start (owner decision, Oct 5 2026): the POS
-- snapshot that ends in the 12 months before the window, the CMI of the fiscal year that ends before it and the model
-- population flags (failure modes 306 to 316).
{{ config(materialized='table') }}

with

windows as (
    -- Calendar-year windows only; rolling and 18-month windows are not spine years [306] [307].
    select
        entity_id as ccn,
        window_start,
        year(window_start) as window_year,
        count(distinct measure_id) as hai_measure_count
    from {{ ref('int_hai_hospital_windows') }}
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
    from {{ ref('int_pos_hospital_snapshots') }}
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
        provider_subtype_code,
        control_type_code,
        bed_count,
        certified_bed_count,
        is_active,
        is_state_or_dc,
        is_connecticut
    from {{ ref('int_pos_hospital_snapshots') }}
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

joined as (
    -- No fallback: a hospital missing from the window's snapshot has no POS values [309] [314]; the CMI is the data year
    -- that ends before the window [310]; a held CMI stays empty [315].
    select
        windows.ccn,
        windows.window_year,
        windows.window_start,
        windows.hai_measure_count,
        pos.period_end as pos_period_end,
        pos.state_code,
        pos.county_fips,
        pos.provider_subtype_code,
        pos.control_type_code,
        pos.bed_count,
        pos.certified_bed_count,
        pos.is_active,
        cmi.cmi,
        cmi.data_fiscal_year as cmi_data_fiscal_year,
        cmi.rule_fiscal_year as cmi_rule_fiscal_year,
        cmi.rule_stage as cmi_rule_stage,
        cmi.cases as cmi_cases,
        pos.ccn is not null as has_pos_snapshot,
        cmi.ccn is not null as has_cmi,
        cmi_holds.ccn is not null as is_cmi_held,
        substr(windows.ccn, 3, 2) = '13' as is_critical_access,
        right(windows.ccn, 1) = 'F' as is_veterans_affairs,
        coalesce(pos.is_state_or_dc, false) as is_state_or_dc,
        coalesce(pos.is_connecticut, false) as is_connecticut,
        coalesce(pos.state_code = 'MD', false) as is_maryland
    from windows
    left join window_snapshots on windows.window_start = window_snapshots.window_start
    left join pos
        on
            windows.ccn = pos.ccn
            and window_snapshots.pos_period_end = pos.period_end
    left join cmi
        on
            windows.ccn = cmi.ccn
            and windows.window_year - 1 = cmi.data_fiscal_year
    left join cmi_holds
        on
            windows.ccn = cmi_holds.ccn
            and windows.window_year - 1 = cmi_holds.fiscal_year
)

-- Primary: IPPS hospitals with a CMI in the 50 states and DC, without Veterans Health Administration or critical access
-- hospitals (Oct 1 2026) and without Maryland, which is paid under its own all-payer model (Oct 5 2026). The sensitivity
-- run adds critical access and Maryland hospitals [313] [317]. Connecticut stays in both [316].
select
    *,
    ccn || ':' || window_year as spine_key,
    has_cmi and is_state_or_dc and not is_veterans_affairs and not is_critical_access and not is_maryland as is_primary_population,
    (has_cmi or is_critical_access) and is_state_or_dc and not is_veterans_affairs as is_sensitivity_population
from joined
