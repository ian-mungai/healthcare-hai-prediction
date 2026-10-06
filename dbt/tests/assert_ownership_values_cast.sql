-- Fails for each owner, enrollment or change-of-ownership column with a non-blank value that does not type: a flag other
-- than Y or N, a date that is not M/D/YYYY or a percentage that is not a plain number from 0 to 100 [366] [368] [369].
with

owner_counts as (
    select
        {%- for column in owner_flag_columns() %}
        count(*) filter (where trim({{ column }}) <> '' and ({{ yes_no(column) }}) is null) as {{ column }},
        {%- endfor %}
        count(*) filter (
            where trim(association_date_owner) <> '' and ({{ month_day_year('association_date_owner') }}) is null
        ) as association_date_owner,
        count(*) filter (
            where trim(percentage_ownership) <> '' and coalesce(({{ strict_number('percentage_ownership') }}) not between 0 and 100, true)
        ) as percentage_ownership
    from {{ ref('stg_cms_hospital_owners') }}
),

enrollment_counts as (
    select
        {%- for column in enrollment_flag_columns() %}
        count(*) filter (where trim({{ column }}) <> '' and ({{ yes_no(column) }}) is null) as {{ column }},
        {%- endfor %}
        count(*) filter (
            where trim(incorporation_date) <> '' and ({{ month_day_year('incorporation_date') }}) is null
        ) as incorporation_date,
        count(*) filter (
            where trim(reh_conversion_date) <> '' and ({{ month_day_year('reh_conversion_date') }}) is null
        ) as reh_conversion_date
    from {{ ref('stg_cms_hospital_enrollments') }}
),

chow_counts as (
    select
        {%- for column in ['multiple_npi_flag_buyer', 'multiple_npi_flag_seller'] %}
        count(*) filter (where trim({{ column }}) <> '' and ({{ yes_no(column) }}) is null) as {{ column }},
        {%- endfor %}
        count(*) filter (
            where trim(effective_date) <> '' and ({{ month_day_year('effective_date') }}) is null
        ) as effective_date
    from {{ ref('stg_cms_change_of_ownership') }}
),

uncast as (
    select
        'owners' as file_kind,
        bronze_column,
        uncast_values
    from (unpivot owner_counts on columns(*) into name bronze_column value uncast_values)
    union all
    select
        'enrollments' as file_kind,
        bronze_column,
        uncast_values
    from (unpivot enrollment_counts on columns(*) into name bronze_column value uncast_values)
    union all
    select
        'changes_of_ownership' as file_kind,
        bronze_column,
        uncast_values
    from (unpivot chow_counts on columns(*) into name bronze_column value uncast_values)
)

select * from uncast
where uncast_values > 0
