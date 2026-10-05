-- Fails for each CMI row whose relative weights divided by its cases differ from its CMI by more than the published
-- rounding: the CMI read is the unadjusted one and the headerless fields are in order [294] [295].
select
    cmi_row_key,
    cmi,
    cases,
    relative_weights
from {{ ref('int_cmi_hospital_rows') }}
where
    cases > 0
    and relative_weights is not null
    and cmi is not null
    and abs(relative_weights / cases - cmi) > 0.0001 + 0.0051 / cases
