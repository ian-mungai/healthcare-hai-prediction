-- Fails for a file whose copies' labels conflict without an owner hold, or a hold on a file without a conflict [181].
select
    file_key,
    has_label_conflict,
    is_label_held
from {{ ref('stg_bronze__files') }}
where has_label_conflict <> is_label_held
