{% macro cmi_year_candidates(year_column) %}
{#- The CMI rows chosen for each year. For a rule year: the files of its best stage. For a data year: the files of the
    latest rule year that carries it, then that rule year's best stage. Each row carries how many CMIs, data years and
    files its CCN-year has [301] [311]. -#}
with

ranked as (
    select
        *,
        {{ year_column }} as fiscal_year,
        -- A later rule year ranks first, then the stage; within one rule year only the stage counts.
        (3000 - rule_fiscal_year) * 10
        + case rule_stage
            when 'correction' then 1
            when 'final' then 2
            when 'interim' then 3
            when 'proposed' then 4
            when 'notice' then 5
            else 6
        end as choice_rank
    from {{ ref('int_cmi_hospital_rows') }}
    where {{ year_column }} is not null
),

best as (
    -- The best choice for the year across its files, not per CCN [301].
    select
        *,
        min(choice_rank) over (partition by fiscal_year) as best_rank
    from ranked
),

chosen as (
    select *
    from best
    where
        choice_rank = best_rank
        and cmi is not null
),

counts as (
    select
        ccn,
        fiscal_year,
        count(distinct cmi) as cmi_values,
        count(distinct coalesce(data_fiscal_year, -1)) as data_years,
        count(distinct member_sha256) as file_count
    from chosen
    group by
        ccn,
        fiscal_year
)

select
    chosen.*,
    counts.cmi_values,
    counts.data_years,
    counts.file_count
from chosen
inner join counts
    on
        chosen.ccn = counts.ccn
        and chosen.fiscal_year = counts.fiscal_year
{% endmacro %}
