-- Fails for each POS control type code whose ownership class differs from the literal table below (design 2.11): 01 to 03 nonprofit, 04 and 09 for profit,
-- 05 to 08 and 10 government, and no class for any other code or for a missing code. The table runs every code through the macro that the spine model uses,
-- so a code that no hospital of the build carries is still checked.
with

codes as (
    select
        code,
        expected_class
    from (
        values
        ('01', 'nonprofit'),
        ('02', 'nonprofit'),
        ('03', 'nonprofit'),
        ('04', 'for_profit'),
        ('05', 'government'),
        ('06', 'government'),
        ('07', 'government'),
        ('08', 'government'),
        ('09', 'for_profit'),
        ('10', 'government'),
        ('00', null),
        ('11', null),
        ('3', null),
        (null, null)
    ) as table_of_codes (code, expected_class)
)

select
    code,
    {{ ownership_class('code') }} as found_class
from codes
where {{ ownership_class('code') }} is distinct from expected_class
