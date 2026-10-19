-- One row per 5-character county FIPS any fact's county key carries, plus the unknown member. The value space is the contract grain: five
-- characters. A value outside that space takes the unknown member in every fact [824] [825]. Three attributes come from the county lists per vintage
-- (the counties and neighbors of the adjacency files, the RUCC counties and the ACS counties): whether the latest adjacency list names the code, the
-- last vintage of any list that names it, and, for a code outside the latest list, the first vintage of any list after that. A code in no list has no
-- vintages. There is no successor column: a successor would be an inference, not a published fact [943].
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
{%- set county_lists = [
    {'model': 'int_county_adjacency_edges', 'column': 'county_fips'},
    {'model': 'int_county_adjacency_edges', 'column': 'neighbor_fips'},
    {'model': 'int_rucc_county_codes', 'column': 'county_fips'},
    {'model': 'int_acs_county_values', 'column': 'county_fips'},
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

lists as (
    {%- for county_list in county_lists %}
    select
        '{{ county_list.model }}' as list_name,
        vintage,
        cast({{ county_list.column }} as varchar) as county_fips
    from {{ ref(county_list.model) }}
    where vintage is not null and {{ county_list.column }} is not null
    {%- if not loop.last %}
    union
    {%- endif %}
    {%- endfor %}
),

vintages as (
    select distinct vintage
    from lists
),

latest_vintage as (
    select max(vintage) as vintage
    from lists
    where list_name = 'int_county_adjacency_edges'
),

latest as (
    select lists.county_fips
    from lists
    inner join latest_vintage
        on lists.vintage = latest_vintage.vintage
    where lists.list_name = 'int_county_adjacency_edges'
),

last_listed as (
    select
        county_fips,
        max(vintage) as last_listed_vintage
    from lists
    group by county_fips
),

first_unlisted as (
    select
        last_listed.county_fips,
        min(vintages.vintage) as first_unlisted_vintage
    from last_listed
    inner join vintages
        on last_listed.last_listed_vintage < vintages.vintage
    where last_listed.county_fips not in (select latest.county_fips from latest)
    group by last_listed.county_fips
),

final as (
    select
        md5(valid.county_fips) as county_sk,
        valid.county_fips,
        latest.county_fips is not null as in_latest_county_list,
        last_listed.last_listed_vintage,
        first_unlisted.first_unlisted_vintage
    from valid
    left join latest
        on valid.county_fips = latest.county_fips
    left join last_listed
        on valid.county_fips = last_listed.county_fips
    left join first_unlisted
        on valid.county_fips = first_unlisted.county_fips
    union all
    select
        md5('<unknown>') as county_sk,
        '<unknown>' as county_fips,
        cast(null as boolean) as in_latest_county_list,
        cast(null as varchar) as last_listed_vintage,
        cast(null as varchar) as first_unlisted_vintage
)

select * from final
