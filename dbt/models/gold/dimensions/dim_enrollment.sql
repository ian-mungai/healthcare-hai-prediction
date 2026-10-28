-- One row per enrollment ID any fact's enrollment key carries, plus the unknown member. The key is the published enrollment ID with no other
-- attribute. A value outside that space takes the unknown member in every fact.
{%- set sources = [
    {'model': 'int_change_of_ownership_rows', 'column': 'enrollment_id_buyer'},
    {'model': 'int_change_of_ownership_rows', 'column': 'enrollment_id_seller'},
    {'model': 'int_hospital_enrollment_rows', 'column': 'enrollment_id'},
    {'model': 'int_hospital_owner_rows', 'column': 'enrollment_id'},
] %}

with

observed as (
    {%- for source in sources %}
    select cast({{ source.column }} as varchar) as enrollment_id
    from {{ ref(source.model) }}
    {%- if not loop.last %}
    union
    {%- endif %}
    {%- endfor %}
),

valid as (
    select enrollment_id
    from observed
    where enrollment_id is not null
),

final as (
    select
        md5(enrollment_id) as enrollment_sk,
        enrollment_id
    from valid
    union all
    select
        md5('<unknown>') as enrollment_sk,
        '<unknown>' as enrollment_id
)

select * from final
