-- A measure row with no edition keeps the identity text and key it had before editions existed: the md5 of the fact, measure key and value column joined by
-- a vertical bar, written here with string concatenation instead of concat_ws. A row with an edition adds the edition start. The keys of the code
-- dimensions are pinned by the surrogate_key_matches_natural_key test of their models [941].
select
    measure_identity,
    fact || '|' || measure_key || '|' || value_column as base_identity
from {{ ref('dim_measure') }}
where
    measure_sk <> md5('<unknown>')
    and edition_start is null
    and (measure_identity <> fact || '|' || measure_key || '|' || value_column or measure_sk <> md5(fact || '|' || measure_key || '|' || value_column))
union all
select
    measure_identity,
    fact || '|' || measure_key || '|' || value_column || '|' || edition_start as base_identity
from {{ ref('dim_measure') }}
where
    measure_sk <> md5('<unknown>')
    and edition_start is not null
    and (
        measure_identity <> fact || '|' || measure_key || '|' || value_column || '|' || edition_start
        or measure_sk <> md5(fact || '|' || measure_key || '|' || value_column || '|' || edition_start)
    )
