-- One row per 2-letter postal code any fact's state key carries, plus the unknown member. The value space is a 2-letter upper-case postal code. A
-- value outside that space takes the unknown member in every fact [824] [825]. state_fips is the first two characters of the county FIPS that
-- int_rucc_county_codes gives the state; a state with no RUCC row (CN and MX) keeps a null state_fips [942].
{%- set sources = [
    {'model': 'int_cost_reports', 'column': 'state_code'},
    {'model': 'int_hai_state_windows', 'column': 'entity_id'},
    {'model': 'int_hgi_hospital_releases', 'column': 'state_code'},
    {'model': 'int_hhs_capacity_weeks', 'column': 'state'},
    {'model': 'int_hospital_enrollment_rows', 'column': 'state'},
    {'model': 'int_mup_providers', 'column': 'state_code'},
    {'model': 'int_onc_chpl_linkage_rows', 'column': 'state'},
    {'model': 'int_pos_hospital_snapshots', 'column': 'state_code'},
    {'model': 'int_rucc_county_codes', 'column': 'state'},
] %}

with

observed as (
    {%- for source in sources %}
    select cast({{ source.column }} as varchar) as state_code
    from {{ ref(source.model) }}
    {%- if not loop.last %}
    union
    {%- endif %}
    {%- endfor %}
),

valid as (
    select state_code
    from observed
    where regexp_matches(state_code, '^[A-Z]{2}$')
),

fips as (
    select distinct
        state as state_code,
        left(county_fips, 2) as state_fips
    from {{ ref('int_rucc_county_codes') }}
    where state is not null and county_fips is not null
),

final as (
    select
        md5(valid.state_code) as state_sk,
        valid.state_code,
        fips.state_fips
    from valid
    left join fips
        on valid.state_code = fips.state_code
    union all
    select
        md5('<unknown>') as state_sk,
        '<unknown>' as state_code,
        cast(null as varchar) as state_fips
)

select * from final
