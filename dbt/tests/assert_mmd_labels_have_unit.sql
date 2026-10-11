-- Fails for an MMD row whose published condition label the reviewed unit seed does not name: the unit is source metadata
-- the file does not print, and no default is applied [472].
select
    mmd_row_key,
    condition_label
from {{ ref('int_mmd_prevalence') }}
where value_unit is null
