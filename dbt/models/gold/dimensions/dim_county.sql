-- One row per 5-character county FIPS any fact's county key carries, plus the unknown member. The value space is the contract grain: five
-- characters. The contract attributes for the Connecticut planning region and the retired Alaska codes are deferred (amendment A36). A value
-- outside that space takes the unknown member in every fact [824] [825].
{%- set sources = [
    {'model': 'int_acs_county_values', 'column': 'county_fips'},
    {'model': 'int_bls_county_series', 'column': 'county_fips'},
    {'model': 'int_gv_county_values', 'column': 'county_fips'},
    {'model': 'int_hpsa_components', 'column': 'county_fips'},
    {'model': 'int_mmd_prevalence', 'column': 'county_fips'},
    {'model': 'int_mua_components', 'column': 'county_fips'},
    {'model': 'int_places_county_values', 'column': 'county_fips'},
    {'model': 'int_pos_hospital_snapshots', 'column': 'county_fips'},
    {'model': 'int_ruca_codes', 'column': 'county_fips'},
    {'model': 'int_rucc_county_codes', 'column': 'county_fips'},
    {'model': 'int_sahie_county_rows', 'column': 'county_fips'},
    {'model': 'int_saipe_county_estimates', 'column': 'county_fips'},
    {'model': 'int_svi_county_values', 'column': 'county_fips'},
    {'model': 'int_wonder_county_deaths', 'column': 'county_fips'},
] %}

with

observed as (
    {%- for source in sources %}
    select cast({{ source.column }} as varchar) as county_fips
    from {{ ref(source.model) }}
    {%- if not loop.last %}
    union
    {%- endif %}
    {%- endfor %}
),

valid as (
    select county_fips
    from observed
    where length(county_fips) = 5
),

final as (
    select
        md5(county_fips) as county_sk,
        county_fips
    from valid
    union all
    select
        md5('<unknown>') as county_sk,
        '<unknown>' as county_fips
)

select * from final
