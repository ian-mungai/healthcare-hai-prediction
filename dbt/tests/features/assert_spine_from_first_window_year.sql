-- Fails for each spine row whose calendar year is before the reviewed first_window_year (dbt_project.yml). Earlier HAI windows stay in
-- fct_hai_hospital_window and feed only the history features; the spine, and so every training row, starts at the reviewed year.
select
    ccn,
    window_year,
    window_start
from {{ ref('int_hospital_spine') }}
where window_year < {{ var('first_window_year') }}
