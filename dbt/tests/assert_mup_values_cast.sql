-- Fails for each Medicare inpatient numeric column with a non-blank value that is not a plain number [359].
with

provider_counts as (
    select
        {%- for column in mup_provider_numeric_columns() %}
        count(*) filter (
            where trim({{ column }}) <> '' and ({{ strict_number(column) }}) is null
        ) as {{ column }},
        {%- endfor %}
        count(*) as provider_rows
    from {{ ref('stg_cms_medicare_inpatient_by_provider') }}
),

provider_uncast as (
    unpivot (select * exclude (provider_rows) from provider_counts)
    on columns(*) into name bronze_column value uncast_values
),

drg_counts as (
    select
        {%- for column in ['tot_dschrgs', 'avg_submtd_cvrd_chrg', 'avg_tot_pymt_amt', 'avg_mdcr_pymt_amt'] %}
        count(*) filter (
            where trim({{ column }}) <> '' and ({{ strict_number(column) }}) is null
        ) as {{ column }},
        {%- endfor %}
        count(*) as drg_rows
    from {{ ref('stg_cms_medicare_inpatient_by_drg') }}
),

drg_uncast as (
    unpivot (select * exclude (drg_rows) from drg_counts)
    on columns(*) into name bronze_column value uncast_values
)

select
    'provider' as file_kind,
    bronze_column,
    uncast_values
from provider_uncast
where uncast_values > 0
union all
select
    'drg' as file_kind,
    bronze_column,
    uncast_values
from drg_uncast
where uncast_values > 0
