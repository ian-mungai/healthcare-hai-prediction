-- Fails for each IPPS data file whose family looks like an impact file but is neither read as one nor reviewed as not
-- one, so a new impact file is never skipped or read silently [342].
select distinct
    bronze_table,
    member_sha256,
    family
from {{ ref('stg_bronze__file_labels') }}
where
    role = 'data'
    and family like '%imp%'
    and family not in {{ quoted_list('impact_families') }}
    and family not in {{ quoted_list('impact_excluded_families') }}
