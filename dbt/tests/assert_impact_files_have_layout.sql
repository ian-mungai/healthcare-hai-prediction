-- Fails for each impact file or sheet that is read but has no header row with exactly one CCN column, or in which two
-- columns map to one field: a layout is never guessed [343] [344] [345].
select *
from {{ ref('int_impact_file_layouts') }}
where ccn_columns <> 1 or repeated_fields > 0
