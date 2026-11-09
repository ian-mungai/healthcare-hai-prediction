-- Fails for each spine row whose log of CMI cases or change of CMI differs from the arithmetic of the CMI data years (design 2.10 and 2.11). The log is null
-- when the cases are not above 0. The change is the CMI of fiscal year Y-1 minus that of Y-2, and null when either year has no CMI: two data years that are
-- one year apart are never more than 18 months apart, and a missing year is never bridged by an older one.

with

expected as (
    select
        spine.spine_key,
        spine.cmi_cases_log,
        spine.cmi_change,
        case when latest.cases > 0 then ln(latest.cases) end as expected_cmi_cases_log,
        latest.cmi - earlier.cmi as expected_cmi_change
    from {{ ref('int_hospital_spine') }} as spine
    left join {{ ref('int_cmi_hospital_data_years') }} as latest
        on
            spine.ccn = latest.ccn
            and spine.window_year - 1 = latest.data_fiscal_year
    left join {{ ref('int_cmi_hospital_data_years') }} as earlier
        on
            spine.ccn = earlier.ccn
            and spine.window_year - 2 = earlier.data_fiscal_year
)

select
    spine_key,
    case
        when (cmi_cases_log is null) <> (expected_cmi_cases_log is null) or abs(cmi_cases_log - expected_cmi_cases_log) > 0.000000001
            then 'wrong log of CMI cases'
        else 'wrong change of CMI'
    end as failure
from expected
where
    (cmi_cases_log is null) <> (expected_cmi_cases_log is null)
    or abs(cmi_cases_log - expected_cmi_cases_log) > 0.000000001
    or (cmi_change is null) <> (expected_cmi_change is null)
    or abs(cmi_change - expected_cmi_change) > 0.000000001
