-- One case-mix index per hospital (CCN) and payment-rule fiscal year, from the files of the year's best rule stage:
-- correction, then final, interim, proposed, notice and unspecified. A correction supersedes the final file of its rule
-- year as a whole, so a CCN only in a lower-stage file gets no CMI for that year. When the best stage's rows disagree on
-- the CMI or the data year, the CCN-year is held in int_cmi_holds instead (failure modes 300 to 302).
{{ config(materialized='table') }}

with

candidates as (
    {{ cmi_year_candidates('rule_fiscal_year') }}
),

picked as (
    select
        *,
        row_number() over (partition by ccn, fiscal_year order by member_sha256, sheet_name, line_number) as pick
    from candidates
    where
        cmi_values = 1
        and data_years = 1
)

select
    ccn,
    rule_fiscal_year,
    data_fiscal_year,
    rule_stage,
    cmi,
    cases,
    relative_weights,
    transfer_adjusted_cmi,
    member_sha256,
    file_count,
    ccn || ':' || rule_fiscal_year as year_key
from picked
where pick = 1
