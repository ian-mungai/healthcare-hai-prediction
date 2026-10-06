-- Fails for each HHS or ONC column with a non-blank value that does not type: an HHS value that is not a plain number (a
-- negative value types and is listed in negative_fields), a week that is not YYYY/MM/DD, a true/false flag, a Y/N
-- criterion, an ONC date that is not M/D/YYYY, a year that is not 4 digits or a month outside 1 to 12 [375] [376] [380]
-- [381].
with

hhs_counts as (
    select
        {%- for column in hhs_numeric_columns() %}
        count(*) filter (where trim({{ column }}) <> '' and ({{ strict_number(column) }}) is null) as {{ column }},
        {%- endfor %}
        count(*) filter (where trim(collection_week) <> '' and ({{ slash_date('collection_week') }}) is null) as collection_week,
        count(*) filter (where trim(is_metro_micro) <> '' and ({{ true_false('is_metro_micro') }}) is null) as is_metro_micro,
        count(*) filter (where trim(is_corrected) <> '' and ({{ true_false('is_corrected') }}) is null) as is_corrected
    from {{ ref('stg_hhs_capacity_csv') }}
),

chpl_counts as (
    select
        {%- set criterion = 'meets_criteria_for_promoting_interoperability_of_ehrs' %}
        count(*) filter (where trim({{ criterion }}) <> '' and ({{ yes_no(criterion) }}) is null) as {{ criterion }},
        count(*) filter (where trim(start_date) <> '' and ({{ month_day_year('start_date') }}) is null) as start_date,
        count(*) filter (where trim(end_date) <> '' and ({{ month_day_year('end_date') }}) is null) as end_date,
        count(*) filter (where trim(year) <> '' and ({{ year_number('year') }}) is null) as program_year
    from {{ ref('stg_onc_pi_chpl_linkage_csv') }}
),

attestation_counts as (
    select
        count(*) filter (where trim(program_year) <> '' and ({{ year_number('program_year') }}) is null) as program_year,
        count(*) filter (where trim(attestation_year) <> '' and ({{ year_number('attestation_year') }}) is null) as attestation_year,
        count(*) filter (where trim(attestation_month) <> '' and ({{ month_number('attestation_month') }}) is null) as attestation_month
    from {{ ref('stg_onc_pi_attestations_csv') }}
),

uncast as (
    select
        'hhs' as file_kind,
        bronze_column,
        uncast_values
    from (unpivot hhs_counts on columns(*) into name bronze_column value uncast_values)
    union all
    select
        'onc_chpl_linkage' as file_kind,
        bronze_column,
        uncast_values
    from (unpivot chpl_counts on columns(*) into name bronze_column value uncast_values)
    union all
    select
        'onc_attestations' as file_kind,
        bronze_column,
        uncast_values
    from (unpivot attestation_counts on columns(*) into name bronze_column value uncast_values)
)

select * from uncast
where uncast_values > 0
