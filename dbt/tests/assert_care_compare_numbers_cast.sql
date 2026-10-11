-- Fails for each Care Compare number column that does not follow the plain-number rule: set from text that is not a plain
-- number (a category, token, exponent, sign, thousands separator or percent sign), null from text that is, or unequal to
-- the text cast to a double. The published text columns stay beside the numbers [327].
with

numbers as (
    select
        'int_cc_timely_effective_windows' as checked_model,
        window_key,
        'score' as field,
        score as value_published,
        score_number as value_number
    from {{ ref('int_cc_timely_effective_windows') }}
    union all
    select
        'int_cc_maternal_windows' as checked_model,
        window_key,
        'score' as field,
        score as value_published,
        score_number as value_number
    from {{ ref('int_cc_maternal_windows') }}
    {%- for field in ['hcahps_answer_percent', 'number_of_completed_surveys', 'survey_response_rate_percent'] %}
    union all
    select
        'int_cc_hcahps_windows' as checked_model,
        window_key,
        '{{ field }}' as field,
        {{ field }} as value_published,
        {{ field }}_number as value_number
    from {{ ref('int_cc_hcahps_windows') }}
    {%- endfor %}
)

select
    checked_model,
    window_key,
    field,
    value_published,
    value_number
from numbers
where
    case
        when regexp_full_match(trim(value_published), '-?[0-9]+(\.[0-9]+)?')
            then value_number is distinct from trim(value_published)::double
        else value_number is not null
    end
