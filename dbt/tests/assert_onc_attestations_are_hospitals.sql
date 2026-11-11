-- Fails for each ONC attestation row whose provider type is not Hospital: the version 2 whole file also holds
-- eligible-professional clinician rows, which are not hospital attestations and must not reach this model.
select
    attestation_key,
    provider_type
from {{ ref('int_onc_attestation_rows') }}
where provider_type is distinct from 'Hospital'
