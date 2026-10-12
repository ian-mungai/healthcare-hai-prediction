-- One row per (rurality code system, code) as published, plus the unknown member. code_system names the silver model and column; code_label is the
-- paired label column where silver has one, else null. A code with two different labels keeps the label of its latest vintage (amendment A48); a
-- code with a label and a null label keeps the label [824] [750].
{%- set code_systems = [
    {'model': 'int_rucc_county_codes', 'code': 'rucc_code', 'label': 'description', 'order': 'vintage'},
    {'model': 'int_ruca_codes', 'code': 'primary_ruca', 'label': ''},
    {'model': 'int_pos_hospital_snapshots', 'code': 'cbsa_urban_rural_code', 'label': ''},
] %}

with

observed as (
    {%- set ns = namespace(first=true) %}
    {%- for code_system in code_systems %}
    {%- if not ns.first %}
    union all
    {%- endif %}
    {%- set ns.first = false %}
    select
        '{{ code_system.model }}.{{ code_system.code }}' as code_system,
        cast({{ code_system.code }} as varchar) as code,
        {{ 'cast(' ~ code_system.label ~ ' as varchar)' if code_system.label else 'cast(null as varchar)' }} as code_label,
        {{ 'cast(' ~ code_system.order ~ ' as varchar)' if code_system.get('order') else 'cast(null as varchar)' }} as label_order
    from {{ ref(code_system.model) }}
    where {{ code_system.code }} is not null
    {%- endfor %}
),

-- A code published with more than one label keeps the label of its latest vintage (then the first label in order); every
-- published label stays on the fact rows (amendment A48).
labeled as (
    select distinct on (code_system, code)
        code_system,
        code,
        code_label
    from observed
    where code_label is not null
    order by code_system asc, code asc, label_order desc nulls last, code_label asc
),

unlabeled as (
    select distinct
        observed.code_system,
        observed.code,
        observed.code_label
    from observed
    where
        observed.code_label is null
        and not exists (
            select 1
            from labeled
            where
                labeled.code_system = observed.code_system
                and labeled.code = observed.code
        )
),

ranked as (
    select * from labeled
    union all
    select * from unlabeled
),

final as (
    select
        md5(concat_ws('|', code_system, code)) as rurality_code_sk,
        code_system,
        code,
        code_label
    from ranked
    union all
    select
        md5('<unknown>') as rurality_code_sk,
        '<unknown>' as code_system,
        '<unknown>' as code,
        cast(null as varchar) as code_label
)

select * from final
