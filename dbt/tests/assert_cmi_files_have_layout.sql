-- Fails for each CMI file or sheet with a header in which the CCN or the CMI column is not found exactly once, or in which
-- two columns map to one field: an unknown layout is never parsed silently [294] [297].
select *
from {{ ref('int_cmi_file_layouts') }}
where
    has_header
    and (ccn_columns <> 1 or cmi_columns <> 1 or repeated_fields > 0)
