-- Fails for each data file of a CMI snapshot whose family is neither read as CMI nor reviewed as not CMI, so a new CMI
-- file is never skipped silently [293].
with

files as (
    {{ cmi_snapshot_files() }}
)

select distinct
    bronze_table,
    member_sha256,
    family
from files
where
    family not in {{ quoted_list('cmi_families') }}
    and family not in {{ quoted_list('cmi_excluded_families') }}
    {%- for pattern in var('cmi_excluded_family_patterns') %}
    and family not like '{{ pattern }}'
    {%- endfor %}
