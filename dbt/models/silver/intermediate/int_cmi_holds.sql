-- Hospital (CCN) and year pairs whose chosen CMI rows disagree on the CMI or the data year: no CMI is chosen. The year
-- basis is rule (int_cmi_hospital_years) or data (int_cmi_hospital_data_years) (failure modes 301 and 312).
{{ config(materialized='table') }}

with

rule_candidates as (
    {{ cmi_year_candidates('rule_fiscal_year') }}
),

data_candidates as (
    {{ cmi_year_candidates('data_fiscal_year') }}
),

candidates as (
    select
        'rule' as year_basis,
        ccn,
        fiscal_year,
        cmi_values,
        data_years,
        file_count
    from rule_candidates
    union all
    select
        'data' as year_basis,
        ccn,
        fiscal_year,
        cmi_values,
        data_years,
        file_count
    from data_candidates
)

select
    year_basis,
    ccn,
    fiscal_year,
    'values_disagree' as hold_reason,
    year_basis || ':' || ccn || ':' || fiscal_year as year_key,
    max(file_count) as file_count,
    count(*) as row_count
from candidates
where
    cmi_values > 1
    or data_years > 1
group by
    year_basis,
    ccn,
    fiscal_year
