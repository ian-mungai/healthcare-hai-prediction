-- One row per owner associate ID any fact's owner key carries, plus the unknown member. The value space is the 10-digit associate ID. Names and
-- flags stay on each release row. A value outside that space takes the unknown member in every fact [824] [825].
{%- set sources = [
    {'model': 'int_change_of_ownership_rows', 'column': 'associate_id_buyer'},
    {'model': 'int_change_of_ownership_rows', 'column': 'associate_id_seller'},
    {'model': 'int_hospital_owner_rows', 'column': 'associate_id'},
] %}

with

observed as (
    {%- for source in sources %}
    select cast({{ source.column }} as varchar) as associate_id
    from {{ ref(source.model) }}
    {%- if not loop.last %}
    union
    {%- endif %}
    {%- endfor %}
),

valid as (
    select associate_id
    from observed
    where regexp_matches(associate_id, '^[0-9]{10}$')
),

final as (
    select
        md5(associate_id) as owner_organization_sk,
        associate_id
    from valid
    union all
    select
        md5('<unknown>') as owner_organization_sk,
        '<unknown>' as associate_id
)

select * from final
