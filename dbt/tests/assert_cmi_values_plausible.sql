-- Fails for each CMI row whose CMI is not above 0 and below 10, or whose published CMI does not cast [296] [303].
select
    cmi_row_key,
    cmi,
    cmi_source_value
from {{ ref('int_cmi_hospital_rows') }}
where
    (cmi is not null and (cmi <= 0 or cmi >= 10))
    or (cmi is null and cmi_source_value is not null)
