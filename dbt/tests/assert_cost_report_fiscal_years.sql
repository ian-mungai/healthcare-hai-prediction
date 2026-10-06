-- Fails for each cost report whose fiscal year (the federal fiscal year in which its period starts) differs from the year of
-- the file that publishes it, or whose period does not parse [332] [333].
select
    rpt_rec_num,
    fiscal_year,
    file_fiscal_year,
    period_begin,
    period_end
from {{ ref('int_cost_reports') }}
where
    fiscal_year is distinct from file_fiscal_year
    or period_begin is null
    or period_end is null
