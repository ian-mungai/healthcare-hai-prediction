-- One row per hospital (CCN) and Provider of Services snapshot: the snapshot period from the catalog, padded codes,
-- 5-character county FIPS, typed counts, switches and dates, and the flags the model population uses (failure modes 280
-- to 292). Every hospital row stays, terminated or outside the 50 states and DC; later models filter.
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        period_start,
        period_end
    from {{ ref('pos_file_periods') }}
),

hospitals as (
    -- Provider category 01 is a hospital in every layout [282].
    select *
    from {{ ref('stg_cms_provider_of_services') }}
    where {{ pos_typed_value('prvdr_ctgry_cd', 'code', 2) }} = '01'
),

typed as (
    select
        _member_sha256 as member_sha256,
        _object_key as object_key,
        _row_number as source_row_number,
        nullif(trim(prvdr_num), '') as provider_number,
        nullif(trim(fac_name), '') as facility_name,
        upper(nullif(trim(state_cd), '')) as state_code,
        case when regexp_full_match(trim(zip_cd), '[0-9]{5}(-?[0-9]{4})?') then left(trim(zip_cd), 5) end as zip_code,
        upper(nullif(trim(cbsa_urbn_rrl_ind), '')) as cbsa_urban_rural_code,
        {%- for column, name, kind, width in pos_typed_columns() %}
        {{ pos_typed_value(column, kind, width) }} as {{ name }},
        {%- endfor %}
        -- A CCN is 6 characters of digits and capital letters, never cast to a number [289].
        case when regexp_full_match(trim(prvdr_num), '[0-9A-Z]{6}') then trim(prvdr_num) end as ccn
    from hospitals
),

final as (
    select
        typed.*,
        periods.period_start,
        periods.period_end,
        coalesce(typed.ccn, typed.provider_number) || ':' || periods.period_end as snapshot_key,
        -- A county needs both padded parts; either missing leaves it null [285].
        typed.fips_state_code || typed.fips_county_code as county_fips,
        typed.ssa_state_code || typed.ssa_county_part as ssa_county_code,
        typed.termination_code = '00' as is_active,
        coalesce(typed.state_code in {{ us_states_and_dc() }}, false) as is_state_or_dc,
        coalesce(typed.state_code = 'CT', false) as is_connecticut,
        coalesce(substr(typed.ccn, 3, 2) = '13', false) as is_critical_access_by_ccn,
        coalesce(right(typed.ccn, 1) = 'F', false) as is_veterans_affairs
    from typed
    -- A file without a period keeps its rows with a null period, which the tests reject [281].
    left join periods on typed.member_sha256 = periods.member_sha256
)

select * from final
