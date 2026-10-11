-- Fails for each ONC attestation whose payment_year does not match its published text: a non-blank text that gives no
-- integer, an integer without its text, or an integer that differs from the text's value (failure mode 795).
select
    attestation_key,
    payment_year_text,
    payment_year
from {{ ref('int_onc_attestation_rows') }}
where
    (payment_year_text is null) <> (payment_year is null)
    or payment_year is distinct from try_cast(payment_year_text as integer)
