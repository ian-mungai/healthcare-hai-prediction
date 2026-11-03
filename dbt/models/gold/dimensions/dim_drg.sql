-- One identity per billed diagnosis-related group code, plus the unknown member; the latest non-null published label is a display attribute.
-- The approved single_latest policy does not establish a grouper version per calendar-year release or a rename-only classification.
{%- set code_systems = [
    {'model': 'int_mup_drg_discharges', 'code': 'drg_cd', 'label': 'drg_desc', 'order': 'data_year'},
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

-- Published labels remain on fact rows; the dimension selects the latest vintage and breaks ties by label.
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

identity_rules as (
    select
        code_system,
        code,
        identity_rule
    from {{ ref('identity_splits') }}
    where dimension = 'dim_drg'
),

final as (
    select
        md5(concat_ws('|', ranked.code_system, ranked.code)) as drg_sk,
        ranked.code_system,
        ranked.code,
        ranked.code_label,
        identity_rules.identity_rule
    from ranked
    left join identity_rules
        on ranked.code_system = identity_rules.code_system and ranked.code = identity_rules.code
    union all
    select
        md5('<unknown>') as drg_sk,
        '<unknown>' as code_system,
        '<unknown>' as code,
        cast(null as varchar) as code_label,
        cast(null as varchar) as identity_rule
)

select * from final
