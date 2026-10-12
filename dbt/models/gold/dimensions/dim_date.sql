-- One row per day from the earliest to the latest value of every source-date column of type DATE that the facts name (read from the kept silver
-- models), plus the unknown member. Year-only columns do not extend the range. The natural key is the date; date_sk is the md5 of its text [824].
-- iso_year and iso_week follow ISO 8601 and the federal fiscal year starts on October 1 (three months later falls in the fiscal year).
{%- set date_sources = [
    {'model': 'int_cc_complications_deaths_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_cc_hcahps_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_cc_hrrp_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_cc_maternal_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_cc_timely_effective_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_cc_unplanned_visits_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_cc_window_holds', 'columns': ['window_end', 'window_start']},
    {'model': 'int_change_of_ownership_rows', 'columns': ['effective_date', 'period_end', 'period_start']},
    {'model': 'int_cost_reports', 'columns': ['period_begin', 'period_end']},
    {
        'model': 'int_hac_program_years',
        'columns': ['hai_measures_end_date', 'hai_measures_start_date', 'psi_90_end_date', 'psi_90_start_date', 'release_date'],
    },
    {'model': 'int_hai_hospital_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_hai_national_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_hai_state_windows', 'columns': ['release_date', 'window_end', 'window_start']},
    {'model': 'int_hai_window_holds', 'columns': ['window_end', 'window_start']},
    {'model': 'int_hgi_hospital_releases', 'columns': ['release_date']},
    {'model': 'int_hhs_capacity_weeks', 'columns': ['collection_week']},
    {'model': 'int_hospital_enrollment_rows', 'columns': ['incorporation_date', 'period_end', 'period_start', 'reh_conversion_date']},
    {'model': 'int_hospital_owner_rows', 'columns': ['association_date', 'period_end', 'period_start']},
    {'model': 'int_hpsa_components', 'columns': ['capture_date', 'designation_date', 'last_update_date', 'withdrawn_date']},
    {'model': 'int_hsa_zip_cases', 'columns': ['period_end', 'period_start']},
    {'model': 'int_mua_components', 'columns': ['capture_date', 'designation_date', 'update_date', 'withdrawal_date']},
    {'model': 'int_occmix_survey_rows', 'columns': ['survey_end_date', 'survey_start_date']},
    {'model': 'int_onc_chpl_linkage_rows', 'columns': ['end_date', 'start_date']},
    {
        'model': 'int_pos_hospital_snapshots',
        'columns': ['certification_date', 'original_participation_date', 'period_end', 'period_start', 'termination_date'],
    },
    {'model': 'int_validation_window_holds', 'columns': ['window_end', 'window_start']},
    {'model': 'int_vbp_program_years', 'columns': ['release_date']},
    {'model': 'int_wonder_county_deaths', 'columns': ['period_end', 'period_start']},
] %}

with

ranges as (
    {%- set ns = namespace(first=true) %}
    {%- for source in date_sources %}
    {%- for source_column in source.columns %}
    {%- if not ns.first %}
    union all
    {%- endif %}
    {%- set ns.first = false %}
    select
        min({{ source_column }}) as first_day,
        max({{ source_column }}) as last_day
    from {{ ref(source.model) }}
    {%- endfor %}
    {%- endfor %}
),

bounds as (
    select
        min(first_day) as first_day,
        max(last_day) as last_day
    from ranges
),

days as (
    select cast(unnest(generate_series(first_day, last_day, interval 1 day)) as date) as calendar_date
    from bounds
),

final as (
    select
        md5(cast(calendar_date as varchar)) as date_sk,
        calendar_date,
        isoyear(calendar_date) as iso_year,
        weekofyear(calendar_date) as iso_week,
        month(calendar_date) as month_number,
        quarter(calendar_date) as quarter_number,
        year(calendar_date) as calendar_year,
        year(date_add(calendar_date, interval 3 month)) as federal_fiscal_year
    from days
    union all
    select
        md5('<unknown>') as date_sk,
        cast(null as date) as calendar_date,
        cast(null as bigint) as iso_year,
        cast(null as bigint) as iso_week,
        cast(null as bigint) as month_number,
        cast(null as bigint) as quarter_number,
        cast(null as bigint) as calendar_year,
        cast(null as bigint) as federal_fiscal_year
)

select * from final
