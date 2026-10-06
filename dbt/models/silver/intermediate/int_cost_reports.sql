-- One row per hospital cost report (HCRIS public use file): the CCN, the federal fiscal year in which the period starts, the
-- period's inclusive days, whether it is a full year, how many reports the CCN has in that fiscal year, ZIP and urban/rural
-- as approved, and every amount typed (failure modes 331 to 338). No report is dropped or merged; the alignment step chooses
-- between a hospital's reports.
{{ config(materialized='table') }}

with

typed as (
    select
        _member_sha256 as member_sha256,
        _object_key as object_key,
        is_label_held,
        nullif(trim(rpt_rec_num), '') as rpt_rec_num,
        nullif(trim(provider_ccn), '') as provider_ccn_raw,
        nullif(trim(hospital_name), '') as hospital_name,
        upper(nullif(trim(state_code), '')) as state_code,
        nullif(trim(county), '') as county,
        nullif(trim(medicare_cbsa_number), '') as medicare_cbsa_number,
        nullif(trim(ccn_facility_type), '') as ccn_facility_type,
        nullif(trim(provider_type), '') as provider_type,
        nullif(trim(type_of_control), '') as type_of_control,
        nullif(trim(zip_code), '') as zip_code_raw,
        nullif(trim(rural_versus_urban), '') as rural_versus_urban_raw,
        try_cast(regexp_extract(_member_path, 'CostReport_([0-9]{4})', 1) as integer) as file_fiscal_year,
        -- Dates are mm/dd/yyyy in every file; anything else is null and fails the fiscal-year test [333].
        case
            when regexp_full_match(trim(fiscal_year_begin_date), '[0-9]{2}/[0-9]{2}/[0-9]{4}')
                then try_strptime(trim(fiscal_year_begin_date), '%m/%d/%Y')::date
        end as period_begin,
        case
            when regexp_full_match(trim(fiscal_year_end_date), '[0-9]{2}/[0-9]{2}/[0-9]{4}')
                then try_strptime(trim(fiscal_year_end_date), '%m/%d/%Y')::date
        end as period_end,
        {%- for column in cost_report_amount_columns() %}
        {{ cost_report_amount(column) }} as {{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref('stg_cms_hospital_cost_reports') }}
),

dated as (
    select
        *,
        case when regexp_full_match(provider_ccn_raw, '[0-9A-Z]{6}') then provider_ccn_raw end as ccn,
        -- A 5-digit ZIP, one trailing hyphen removed, or the first 5 digits of a hyphenated ZIP+4; others stay raw [337].
        case when regexp_full_match(zip_code_raw, '[0-9]{5}(-|-[0-9]{4})?') then left(zip_code_raw, 5) end as zip_code,
        case upper(rural_versus_urban_raw) when 'U' then 'urban' when 'R' then 'rural' end as rural_urban,
        -- The federal fiscal year in which the period starts: October to December count toward the next year [332].
        year(period_begin) + case when month(period_begin) >= 10 then 1 else 0 end as fiscal_year,
        date_diff('day', period_begin, period_end) + 1 as reporting_days,
        -- A full year ends the day before the start's anniversary; a 29 February start's anniversary is 1 March [334].
        coalesce(
            (period_end + interval 1 day)::date = case
                when strftime(period_begin, '%m-%d') = '02-29' then make_date(year(period_begin) + 1, 3, 1)
                else (period_begin + interval 1 year)::date
            end,
            false
        ) as is_full_year
    from typed
)

select
    *,
    count(*) over (partition by ccn, fiscal_year) as reports_in_fiscal_year
from dated
