-- Fails for each numeric impact-file value that is neither blank, a SAS missing dot nor a plain number [349].
select
    member_sha256,
    sheet_name,
    ccn,
    field,
    value_text
from {{ ref('int_impact_hospital_values') }}
where
    is_numeric_field
    and value_text <> '.'
    and value_number is null
