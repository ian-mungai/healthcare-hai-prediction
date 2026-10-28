-- One row per (rurality code system, code) as published, plus the unknown member. code_system names the silver model and column; code_label is the
-- paired label column where silver has one, else null. A code with two different labels keeps the label of its latest vintage; a
-- code with a label and a null label keeps the label [824] [750]. A code that the identity_splits seed lists as split (a RUCC code whose published meaning
-- changed between editions) has one row per label: edition_start and edition_end are the first and last vintage that carry the label, and the key adds
-- the edition start. Every other row has a null edition and keeps the key it had before editions existed [940] [941].
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

identity_rules as (
    select
        code_system,
        code,
        identity_rule
    from {{ ref('identity_splits') }}
    where dimension = 'dim_rurality_code'
),

split_keys as (
    select
        code_system,
        code
    from identity_rules
    where identity_rule = 'split'
),

-- A code that is not split: published with more than one label, it keeps the label of its latest vintage (then the first label in order); every
-- published label stays on the fact rows.
single as (
    select
        observed.code_system,
        observed.code,
        observed.code_label,
        observed.label_order
    from observed
    left join split_keys
        on observed.code_system = split_keys.code_system and observed.code = split_keys.code
    where split_keys.code is null
),

labeled as (
    select distinct on (code_system, code)
        code_system,
        code,
        code_label
    from single
    where code_label is not null
    order by code_system asc, code asc, label_order desc nulls last, code_label asc
),

unlabeled as (
    select distinct
        single.code_system,
        single.code,
        single.code_label
    from single
    where
        single.code_label is null
        and not exists (
            select 1
            from labeled
            where
                labeled.code_system = single.code_system
                and labeled.code = single.code
        )
),

ranked as (
    select
        code_system,
        code,
        code_label,
        cast(null as varchar) as edition_start,
        cast(null as varchar) as edition_end
    from labeled
    union all
    select
        code_system,
        code,
        code_label,
        cast(null as varchar) as edition_start,
        cast(null as varchar) as edition_end
    from unlabeled
),

-- A split code has one row per published label; the label's first and last vintage bound the edition.
editions as (
    select
        observed.code_system,
        observed.code,
        observed.code_label,
        min(observed.label_order) as edition_start,
        max(observed.label_order) as edition_end
    from observed
    inner join split_keys
        on observed.code_system = split_keys.code_system and observed.code = split_keys.code
    where observed.code_label is not null and observed.label_order is not null
    group by observed.code_system, observed.code, observed.code_label
),

combined as (
    select * from ranked
    union all
    select * from editions
),

final as (
    select
        md5(concat_ws('|', combined.code_system, combined.code, combined.edition_start)) as rurality_code_sk,
        combined.code_system,
        combined.code,
        combined.code_label,
        combined.edition_start,
        combined.edition_end,
        identity_rules.identity_rule
    from combined
    left join identity_rules
        on combined.code_system = identity_rules.code_system and combined.code = identity_rules.code
    union all
    select
        md5('<unknown>') as rurality_code_sk,
        '<unknown>' as code_system,
        '<unknown>' as code,
        cast(null as varchar) as code_label,
        cast(null as varchar) as edition_start,
        cast(null as varchar) as edition_end,
        cast(null as varchar) as identity_rule
)

select * from final
