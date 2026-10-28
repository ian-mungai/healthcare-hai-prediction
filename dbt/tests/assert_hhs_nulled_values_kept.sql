-- Fails for each HHS numeric field or row whose typed values or kept rejected texts differ from what the published text
-- requires. For all 114 fields the expected typed value is the plain number of the
-- published text when it is 0 or more, else null; a non-blank published value other than -999999 whose expected value is
-- null must appear in nulled_values_published under its field with the untrimmed published text; the object holds
-- exactly those fields and is the text {} when there are none. Each kept value must be a JSON string: a number that converts
-- to the same text (-9 for "-9") is a different JSON value and fails.
with

typed as (
    select * from {{ ref('int_hhs_capacity_weeks') }}
),

published as (
    select * from {{ ref('stg_hhs_capacity_csv') }}
),

paired as (
    select
        typed.member_sha256,
        typed.source_row_number,
        typed.nulled_values_published,
        {%- for column in hhs_numeric_columns() %}
        typed.{{ column }} as typed_{{ loop.index }},
        published.{{ column }} as published_{{ loop.index }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from typed
    inner join published
        on
            typed.member_sha256 = published._member_sha256
            and typed.source_row_number = published._row_number
),

expected as (
    select
        member_sha256,
        source_row_number,
        nulled_values_published,
        {%- for column in hhs_numeric_columns() %}
        typed_{{ loop.index }},
        published_{{ loop.index }},
        case
            when
                regexp_full_match(trim(published_{{ loop.index }}), '-?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][-+]?[0-9]+)?')
                and try_cast(trim(published_{{ loop.index }}) as double) >= 0
                then try_cast(trim(published_{{ loop.index }}) as double)
        end as expected_{{ loop.index }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from paired
),

flagged as (
    select
        *,
        {%- for column in hhs_numeric_columns() %}
        coalesce(
            nullif(trim(published_{{ loop.index }}), '') is not null
            and trim(published_{{ loop.index }}) <> '-999999'
            and expected_{{ loop.index }} is null,
            false
        ) as is_nulled_{{ loop.index }},
        {%- endfor %}
        try_cast(nulled_values_published as json) as nulled
    from expected
),

row_checks as (
    select
        *,
        list_sort(list_filter([
            {%- for column in hhs_numeric_columns() %}
            case when is_nulled_{{ loop.index }} then '{{ column }}' end{% if not loop.last %},{% endif %}
            {%- endfor %}
        ], field -> field is not null)) as expected_keys
    from flagged
),

field_problems as (
    select
        {%- for column in hhs_numeric_columns() %}
        count(*) filter (
            where
            typed_{{ loop.index }} is distinct from expected_{{ loop.index }}
            or (
                is_nulled_{{ loop.index }}
                and (
                    json_type(nulled, '$."{{ column }}"') is distinct from 'VARCHAR'
                    or (nulled ->> '{{ column }}') is distinct from published_{{ loop.index }}
                )
            )
        ) as {{ column }},
        {%- endfor %}
        count(*) filter (
            where
            nulled is null
            or json_type(nulled) <> 'OBJECT'
            or list_sort(json_keys(nulled)) <> expected_keys
            or (len(expected_keys) = 0 and nulled_values_published <> '{}')
        ) as nulled_values_object
    from row_checks
),

problems as (
    select
        field,
        rows_with_problem
    from (unpivot field_problems on columns(*) into name field value rows_with_problem)
)

select * from problems
where rows_with_problem > 0
