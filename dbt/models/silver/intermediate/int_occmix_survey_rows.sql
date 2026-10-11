-- Public survey rows at file, sheet and source-row grain. Survey periods are independent of payment rule years.
-- Deleted rows and ambiguous provider-period rows stay visible with their hold reason; the wage and share formulas are
-- saved in features/.
{{ config(materialized='table') }}

with

mapped as (
    select
        survey.bronze_table,
        survey.member_sha256,
        survey.sheet_name,
        survey.source_row_number,
        survey.source_row_key as survey_row_key,
        survey.rule_fiscal_years,
        survey.rule_stages,
        survey.is_deleted or layouts.is_deleted_layout as is_deleted,
        survey.field_values,
    {% for field in ['prov', 'from', 'to', 'rnsal', 'rnhr', 'lpnsthr', 'naorathr', 'mahr', 'nursehr', 'rnahw'] %}
    nullif(trim(trim(trim(survey.field_values[list_position(layouts.header_fields, '{{ field }}')]), '"')), '')
        as {{ field }}_text{% if not loop.last %},{% endif %}
    {% endfor %}
    from {{ ref('int_occmix_file_rows') }} as survey
    inner join {{ ref('int_occmix_file_layouts') }} as layouts
        on
            survey.bronze_table = layouts.bronze_table
            and survey.member_sha256 = layouts.member_sha256
            and survey.sheet_name = layouts.sheet_name
    where
        layouts.is_survey_layout
        and layouts.repeated_fields = 0
        and survey.source_row_number > layouts.header_row_number
),

typed as (
    select
        *,
        prov_text as ccn_published,
        {{ published_ccn('prov_text') }} as ccn,
        {{ occmix_date('from_text') }} as survey_start_date,
        {{ occmix_date('to_text') }} as survey_end_date,
        {% for field in ['rnsal', 'rnhr', 'lpnsthr', 'naorathr', 'mahr', 'nursehr', 'rnahw'] %}
        {{ occmix_number(field ~ '_text') }} as {{ field }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from mapped
),

counted as (
    select
        *,
        count(*) over (partition by bronze_table, member_sha256, sheet_name, ccn, survey_start_date, survey_end_date) as rows_for_provider_period
    from typed
),

held as (
    select
        *,
        case
            when is_deleted then 'deleted_survey_record'
            when ccn is null then 'invalid_provider_id'
            when survey_start_date is null or survey_end_date is null or survey_start_date > survey_end_date then 'invalid_survey_period'
            when rows_for_provider_period > 1 then 'duplicate_provider_period'
        end as hold_reason
    from counted
)

select * from held
