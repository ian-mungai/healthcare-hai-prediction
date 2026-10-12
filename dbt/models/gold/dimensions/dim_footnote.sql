-- One row per (Care Compare footnote code system, code) as published, plus the unknown member. code_system names the silver model and column;
-- code_label is the paired label column where silver has one, else null. A code with two different labels appears twice and fails the uniqueness
-- test; a code with a label and a null label keeps the label [824] [750].
{%- set code_systems = [
    {'model': 'int_cc_complications_deaths_windows', 'code': 'footnote', 'label': ''},
    {'model': 'int_cc_hcahps_windows', 'code': 'hcahps_answer_percent_footnote', 'label': ''},
    {'model': 'int_cc_hcahps_windows', 'code': 'number_of_completed_surveys_footnote', 'label': ''},
    {'model': 'int_cc_hcahps_windows', 'code': 'patient_survey_star_rating_footnote', 'label': ''},
    {'model': 'int_cc_hcahps_windows', 'code': 'survey_response_rate_percent_footnote', 'label': ''},
    {'model': 'int_cc_hrrp_windows', 'code': 'footnote', 'label': ''},
    {'model': 'int_cc_maternal_windows', 'code': 'footnote', 'label': ''},
    {'model': 'int_cc_timely_effective_windows', 'code': 'footnote', 'label': ''},
    {'model': 'int_cc_unplanned_visits_windows', 'code': 'footnote', 'label': ''},
    {'model': 'int_hac_program_years', 'code': 'payment_reduction_footnote', 'label': ''},
    {'model': 'int_hac_program_years', 'code': 'total_hac_score_footnote', 'label': ''},
    {'model': 'int_hgi_hospital_releases', 'code': 'overall_rating_footnote', 'label': ''},
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
        md5(concat_ws('|', code_system, code)) as footnote_sk,
        code_system,
        code,
        code_label
    from ranked
    union all
    select
        md5('<unknown>') as footnote_sk,
        '<unknown>' as code_system,
        '<unknown>' as code,
        cast(null as varchar) as code_label
)

select * from final
