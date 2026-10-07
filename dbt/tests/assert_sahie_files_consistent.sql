-- Fails for a staged SAHIE file whose preamble does not define code 0 of each category as under 65, all races, both sexes
-- and all incomes, and for a county row whose year differs from its file's [438] [439].
with

files as (
    select
        files.member_sha256,
        files.canonical_object_key
    from {{ ref('stg_bronze__files') }} as files
    where files.bronze_table = 'sahie'
),

preambles as (
    select
        _object_key as object_key,
        trim(line_text) as line_text
    from {{ source('bronze', 'file_preambles') }}
),

definitions as (
    select
        files.member_sha256,
        count(distinct preambles.line_text) as defined
    from files
    left join preambles
        on
            files.canonical_object_key = preambles.object_key
            and preambles.line_text in ('0 - Under 65 years', '0 - All races', '0 - Both sexes', '0 - All income levels')
    group by files.member_sha256
)

select
    member_sha256,
    'category codes: ' || defined || ' of 4 defined' as problem
from definitions
where defined <> 4
union all
select
    member_sha256,
    'year ' || coalesce(estimate_year::varchar, 'missing') || ' in a ' || file_year || ' file' as problem
from {{ ref('int_sahie_county_rows') }}
where estimate_year is distinct from file_year
