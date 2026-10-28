-- One row per 5-digit ZIP code any fact's ZIP key carries, plus the unknown member. The value space is five digits. A value outside that space
-- takes the unknown member in every fact.
{%- set sources = [
    {'model': 'int_cost_reports', 'column': 'zip_code'},
    {'model': 'int_hgi_hospital_releases', 'column': 'zip_code'},
    {'model': 'int_hhs_capacity_weeks', 'column': 'zip'},
    {'model': 'int_hospital_enrollment_rows', 'column': 'zip_code'},
    {'model': 'int_hsa_zip_cases', 'column': 'zip_code'},
    {'model': 'int_hud_zip_county_holds', 'column': 'zip_code'},
    {'model': 'int_onc_attestation_rows', 'column': 'zip'},
    {'model': 'int_onc_chpl_linkage_rows', 'column': 'zip_code'},
    {'model': 'int_pos_hospital_snapshots', 'column': 'zip_code'},
] %}

with

observed as (
    {%- for source in sources %}
    select cast({{ source.column }} as varchar) as zip_code
    from {{ ref(source.model) }}
    {%- if not loop.last %}
    union
    {%- endif %}
    {%- endfor %}
),

valid as (
    select zip_code
    from observed
    where regexp_matches(zip_code, '^[0-9]{5}$')
),

final as (
    select
        md5(zip_code) as zip_sk,
        zip_code
    from valid
    union all
    select
        md5('<unknown>') as zip_sk,
        '<unknown>' as zip_code
)

select * from final
