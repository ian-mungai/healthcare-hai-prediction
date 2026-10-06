-- Fails for each staged owner row whose owner type is not O: the acquisition keeps organisation owners only, and staging
-- must never carry an individual owner [367].
select
    member_sha256,
    source_row_number,
    type_owner
from {{ ref('int_hospital_owner_rows') }}
where coalesce(type_owner, '') <> 'O'
