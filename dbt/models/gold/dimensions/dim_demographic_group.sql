-- One row per (SAHIE and Geographic Variation demographic group code system, code) as published, plus the unknown member. code_system names the
-- silver model and column; code_label is the paired label column where silver has one, else null. A code with two different labels appears twice
-- and fails the uniqueness test; a code with a label and a null label keeps the label [824] [750].
{%- set code_systems = [
    {'model': 'int_sahie_county_rows', 'code': 'agecat', 'label': ''},
    {'model': 'int_sahie_county_rows', 'code': 'racecat', 'label': ''},
    {'model': 'int_sahie_county_rows', 'code': 'sexcat', 'label': ''},
    {'model': 'int_sahie_county_rows', 'code': 'iprcat', 'label': ''},
    {'model': 'int_gv_county_values', 'code': 'bene_age_lvl', 'label': ''},
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
        {{ 'cast(' ~ code_system.label ~ ' as varchar)' if code_system.label else 'cast(null as varchar)' }} as code_label
    from {{ ref(code_system.model) }}
    where {{ code_system.code }} is not null
    {%- endfor %}
),

labeled as (
    select distinct
        code_system,
        code,
        code_label
    from observed
    where code_label is not null
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
        md5(concat_ws('|', code_system, code)) as demographic_group_sk,
        code_system,
        code,
        code_label
    from ranked
    union all
    select
        md5('<unknown>') as demographic_group_sk,
        '<unknown>' as code_system,
        '<unknown>' as code,
        cast(null as varchar) as code_label
)

select * from final
