-- One case-mix index per hospital (CCN) and discharge (data) fiscal year, from the latest rule year that publishes the data
-- year, then that rule year's best stage; only data years stated in the file names count. When those rows disagree, the
-- CCN-year is held in int_cmi_holds instead (failure modes 310 to 312).
{{ config(materialized='table') }}

with

candidates as (
    {{ cmi_year_candidates('data_fiscal_year') }}
),

picked as (
    select
        *,
        row_number() over (partition by ccn, fiscal_year order by member_sha256, sheet_name, line_number) as pick
    from candidates
    where cmi_values = 1
)

select
    ccn,
    data_fiscal_year,
    rule_fiscal_year,
    rule_stage,
    cmi,
    cases,
    relative_weights,
    transfer_adjusted_cmi,
    member_sha256,
    file_count,
    ccn || ':' || data_fiscal_year as year_key
from picked
where pick = 1
