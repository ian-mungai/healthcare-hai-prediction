{% macro gold_fact(silver_model, grain, surrogate_keys) %}
{#-
    The body of a gold fact: every column of the kept silver model unchanged and in its order, then one <dimension>_sk column per mapping in
    surrogate_keys, in the order given. A mapping is {'<key name>': {'dimension': 'dim_x', 'natural_key': '<silver column>'}}, as in the gold
    contract. The key is the md5 of the natural key's text, matched to the dimension's own surrogate key (the dimension named dim_x holds
    x_sk); a null or unmatched natural key takes the unknown member, md5('<unknown>') [745]. Each mapping joins its own alias, so one
    dimension can serve two keys (hospital_buyer_sk and hospital_seller_sk on dim_hospital). The join cannot add or drop rows because every
    dimension holds one row per surrogate key. grain is the silver unique key column: the model's tests check it, and the macro stops a build
    that names none [743].
-#}
{{- gold_fact_select(ref(silver_model), silver_model, grain, surrogate_keys) }}
{% endmacro %}


{% macro gold_fact_select(source_relation, silver_model, grain, surrogate_keys) %}
{#- The body of gold_fact over any relation; the key test gold_fact_keys feeds it a fixture relation to check the keys it emits [825]. -#}
{%- if not grain %}
    {{- exceptions.raise_compiler_error('gold_fact(' ~ silver_model ~ '): the grain column is required') }}
{%- endif %}
select
    silver.*
    {%- for key_name, mapping in surrogate_keys.items() %}
    {%- if not key_name.endswith('_sk') or not mapping.dimension.startswith('dim_') or not mapping.natural_key or key_name == grain %}
    {{- exceptions.raise_compiler_error('gold_fact(' ~ silver_model ~ '): bad mapping ' ~ key_name) }}
    {%- endif %},
    coalesce({{ key_name }}_dim.{{ mapping.dimension[4:] }}_sk, md5('<unknown>')) as {{ key_name }}
    {%- endfor %}
from {{ source_relation }} as silver
{%- for key_name, mapping in surrogate_keys.items() %}
left join {{ ref(mapping.dimension) }} as {{ key_name }}_dim
    on md5(cast(silver.{{ mapping.natural_key }} as varchar)) = {{ key_name }}_dim.{{ mapping.dimension[4:] }}_sk
{%- endfor %}
{% endmacro %}


{% macro gold_measure_rows(identities, names, units, value_kinds) %}
{#- The ordinary rows of dim_measure from four relations of the model's steps; the test measure_attribute_branches feeds it fixture rows [828]. -#}
select
    md5(concat_ws('|', identities.fact, identities.measure_key, identities.value_column)) as measure_sk,
    concat_ws('|', identities.fact, identities.measure_key, identities.value_column) as measure_identity,
    identities.fact,
    identities.measure_key,
    identities.value_column,
    names.measure_name as measure_label,
    units.value_unit as unit,
    value_kinds.value_kind,
    identities.vintage_start,
    identities.vintage_end
from {{ identities }} as identities
left join {{ names }} as names
    on
        identities.fact = names.fact
        and identities.measure_key = names.measure_key
left join {{ units }} as units
    on
        identities.fact = 'fct_mmd_prevalence'
        and identities.measure_key = units.measure_key
left join {{ value_kinds }} as value_kinds
    on
        identities.fact = value_kinds.fact
        and identities.value_column = value_kinds.value_column
{% endmacro %}


{% test unknown_member_once(model, column_name) %}
{#- Fails unless the dimension holds exactly one unknown member row [824]. -#}
select count(*) as unknown_members
from {{ model }}
where {{ column_name }} = md5('<unknown>')
having count(*) <> 1
{% endtest %}


{% test unknown_member_natural_key(model, column_name, natural_key, expected="'<unknown>'") %}
{#-
    Fails for an unknown member row (key md5('<unknown>')) whose natural key is not the expected value: the text '<unknown>' for every
    dimension. Amendment A40 item 1 allows one exception: dim_date, whose natural key is a DATE, passes expected="null" and holds a null
    calendar_date on its unknown row [824].
-#}
select *
from {{ model }}
where {{ column_name }} = md5('<unknown>') and {{ natural_key }} is distinct from {{ expected }}
{% endtest %}


{% test surrogate_key_matches_natural_key(model, column_name, natural_key) %}
{#- Fails for an ordinary row whose key is not the md5 of its natural key's text (a wrong but unique hash passes unique and not_null) [824]. -#}
select
    {{ column_name }} as surrogate_key,
    {{ natural_key }} as natural_key
from {{ model }}
where
    {{ column_name }} <> md5('<unknown>')
    and {{ column_name }} is distinct from md5(cast({{ natural_key }} as varchar))
{% endtest %}


{% test dimension_membership(model, column_name, sk_column, sources, space) %}
{#-
    Fails for a natural key value that a source carries inside the dimension's value space and the dimension lacks, and for an ordinary
    dimension row that no source carries inside the space. sources is a list of {model, column, where} entries; space is a condition on the
    column natural_key [824].
-#}
with

source_values as (
    select natural_key
    from (
        {%- for source in sources %}
        select cast({{ source.column }} as varchar) as natural_key
        from {{ ref(source.model) }}
        {%- if source.where %}
        where {{ source.where }}
        {%- endif %}
        {%- if not loop.last %}
        union
        {%- endif %}
        {%- endfor %}
    ) as observed
    where natural_key is not null and ({{ space }})
),

dimension_values as (
    select cast({{ column_name }} as varchar) as natural_key
    from {{ model }}
    where {{ sk_column }} <> md5('<unknown>')
)

select
    'source value missing from the dimension' as problem,
    natural_key
from (
    select natural_key from source_values
    except
    select natural_key from dimension_values
) as missing
union all
select
    'dimension value no source carries' as problem,
    natural_key
from (
    select natural_key from dimension_values
    except
    select natural_key from source_values
) as invented
{% endtest %}


{% test gold_fact_keys(model, column_name) %}
{#-
    Runs gold_fact_select over fixture rows (a known pair of hospitals and their swap, null keys, out-of-space values and unmatched values
    for the role-playing pair hospital_buyer_sk and hospital_seller_sk, and a known and an unmatched release) and compares every emitted key
    with a key computed without the macro: md5 of the text when the dimension holds the value, else md5('<unknown>'). Fails on any
    difference and when the rows are not the fixture's rows [825] [826].
-#}
{%- set dimension_hospital = ref('dim_hospital') %}
{%- set dimension_release = ref('dim_release') %}
with

known as (
    select
        min(ccn) as first_ccn,
        max(ccn) as second_ccn
    from {{ dimension_hospital }}
    where hospital_sk <> md5('<unknown>')
),

known_release as (
    select min(member_sha256) as first_release
    from {{ dimension_release }}
    where release_sk <> md5('<unknown>')
),

cases as (
    select
        'known_pair' as case_name,
        first_ccn as ccn_buyer,
        second_ccn as ccn_seller,
        first_release as member_sha256
    from known
    cross join known_release
    union all
    select
        'swapped_pair' as case_name,
        second_ccn as ccn_buyer,
        first_ccn as ccn_seller,
        first_release as member_sha256
    from known
    cross join known_release
    union all
    select
        'null_keys' as case_name,
        cast(null as varchar) as ccn_buyer,
        cast(null as varchar) as ccn_seller,
        cast(null as varchar) as member_sha256
    union all
    select
        'out_of_space' as case_name,
        '12345' as ccn_buyer,
        '1234567' as ccn_seller,
        'not a checksum' as member_sha256
    union all
    select
        'unmatched' as case_name,
        'NOTFND' as ccn_buyer,
        'NOTFN2' as ccn_seller,
        'unmatched checksum' as member_sha256
    union all
    select
        'known_and_null' as case_name,
        first_ccn as ccn_buyer,
        cast(null as varchar) as ccn_seller,
        first_release as member_sha256
    from known
    cross join known_release
),

actual as (
    {{ gold_fact_select(
        'cases',
        'gold_fact_keys',
        'case_name',
        {
            'hospital_buyer_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn_buyer'},
            'hospital_seller_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn_seller'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        }
    ) }}
),

expected as (
    select
        case_name,
        case when ccn_buyer in (select ccn from {{ dimension_hospital }}) then md5(ccn_buyer) else md5('<unknown>') end as hospital_buyer_sk,
        case when ccn_seller in (select ccn from {{ dimension_hospital }}) then md5(ccn_seller) else md5('<unknown>') end as hospital_seller_sk,
        case when member_sha256 in (select member_sha256 from {{ dimension_release }}) then md5(member_sha256) else md5('<unknown>') end as release_sk
    from cases
)

select
    actual.case_name,
    actual.hospital_buyer_sk,
    actual.hospital_seller_sk,
    actual.release_sk
from actual
inner join expected
    on actual.case_name = expected.case_name
where
    actual.hospital_buyer_sk is distinct from expected.hospital_buyer_sk
    or actual.hospital_seller_sk is distinct from expected.hospital_seller_sk
    or actual.release_sk is distinct from expected.release_sk
union all
select
    'row count' as case_name,
    cast(null as varchar) as hospital_buyer_sk,
    cast(null as varchar) as hospital_seller_sk,
    cast(null as varchar) as release_sk
where (select count(*) from actual) <> (select count(*) from cases)
union all
select
    'no known hospital' as case_name,
    cast(null as varchar) as hospital_buyer_sk,
    cast(null as varchar) as hospital_seller_sk,
    cast(null as varchar) as release_sk
where (select count(first_ccn) from known) = 0
{% endtest %}


{% test dim_date_attributes(model) %}
{#-
    Checks every ordinary day against attributes computed by another route (strftime: ISO year %G, ISO week %V, month, quarter, year and the
    October 1 federal fiscal year), four fixed dates against literal values, and that the days have no gap [824].
-#}
with

ordinary as (
    select
        calendar_date,
        iso_year,
        iso_week,
        month_number,
        quarter_number,
        calendar_year,
        federal_fiscal_year
    from {{ model }}
    where calendar_date is not null
),

expected as (
    select
        calendar_date,
        cast(strftime(calendar_date, '%G') as bigint) as iso_year,
        cast(strftime(calendar_date, '%V') as bigint) as iso_week,
        cast(strftime(calendar_date, '%m') as bigint) as month_number,
        (cast(strftime(calendar_date, '%m') as bigint) + 2) // 3 as quarter_number,
        cast(strftime(calendar_date, '%Y') as bigint) as calendar_year,
        cast(strftime(calendar_date, '%Y') as bigint) + case when cast(strftime(calendar_date, '%m') as bigint) >= 10 then 1 else 0 end as federal_fiscal_year
    from ordinary
),

literals as (
    select
        cast(calendar_date as date) as calendar_date,
        iso_year,
        iso_week,
        month_number,
        quarter_number,
        calendar_year,
        federal_fiscal_year
    from (
        values
            ('2021-01-03', 2020, 53, 1, 1, 2021, 2021),
            ('2024-12-30', 2025, 1, 12, 4, 2024, 2025),
            ('2023-09-30', 2023, 39, 9, 3, 2023, 2023),
            ('2023-10-01', 2023, 39, 10, 4, 2023, 2024)
    ) as fixed (calendar_date, iso_year, iso_week, month_number, quarter_number, calendar_year, federal_fiscal_year)
)

select
    'attribute differs from the strftime route' as problem,
    cast(ordinary.calendar_date as varchar) as detail
from ordinary
inner join expected
    on ordinary.calendar_date = expected.calendar_date
where
    ordinary.iso_year is distinct from expected.iso_year
    or ordinary.iso_week is distinct from expected.iso_week
    or ordinary.month_number is distinct from expected.month_number
    or ordinary.quarter_number is distinct from expected.quarter_number
    or ordinary.calendar_year is distinct from expected.calendar_year
    or ordinary.federal_fiscal_year is distinct from expected.federal_fiscal_year
union all
select
    'attribute differs from the fixed value' as problem,
    cast(ordinary.calendar_date as varchar) as detail
from ordinary
inner join literals
    on ordinary.calendar_date = literals.calendar_date
where
    ordinary.iso_year is distinct from literals.iso_year
    or ordinary.iso_week is distinct from literals.iso_week
    or ordinary.month_number is distinct from literals.month_number
    or ordinary.quarter_number is distinct from literals.quarter_number
    or ordinary.calendar_year is distinct from literals.calendar_year
    or ordinary.federal_fiscal_year is distinct from literals.federal_fiscal_year
union all
select
    'days are not consecutive' as problem,
    cast(count(*) as varchar) as detail
from ordinary
having count(*) <> date_diff('day', min(calendar_date), max(calendar_date)) + 1
{% endtest %}


{% test hospital_seen_dates(model, sources) %}
{#-
    Recomputes first_seen and last_seen per hospital by another route (the smallest and largest date of each source row with least and
    greatest, then over the rows) and fails on any difference or when no hospital has a date [824].
-#}
with

row_dates as (
    {%- for source in sources %}
    select
        cast({{ source.column }} as varchar) as ccn,
        {%- if source.dates %}
        least({{ source.dates | join(', ') }}) as first_day,
        greatest({{ source.dates | join(', ') }}) as last_day
        {%- else %}
        cast(null as date) as first_day,
        cast(null as date) as last_day
        {%- endif %}
    from {{ ref(source.model) }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

expected as (
    select
        ccn,
        min(first_day) as first_seen,
        max(last_day) as last_seen
    from row_dates
    where length(ccn) = 6
    group by ccn
)

select
    coalesce(expected.ccn, actual.ccn) as ccn,
    expected.first_seen as expected_first_seen,
    actual.first_seen as actual_first_seen,
    expected.last_seen as expected_last_seen,
    actual.last_seen as actual_last_seen
from expected
full outer join (
    select
        ccn,
        first_seen,
        last_seen
    from {{ model }}
    where hospital_sk <> md5('<unknown>')
) as actual
    on expected.ccn = actual.ccn
where
    expected.ccn is null
    or actual.ccn is null
    or expected.first_seen is distinct from actual.first_seen
    or expected.last_seen is distinct from actual.last_seen
union all
select
    'no hospital has a first_seen' as ccn,
    cast(null as date) as expected_first_seen,
    cast(null as date) as actual_first_seen,
    cast(null as date) as expected_last_seen,
    cast(null as date) as actual_last_seen
where (select count(first_seen) from {{ model }}) = 0
{% endtest %}


{% test measure_attribute_branches(model) %}
{#-
    Runs gold_measure_rows over fixture rows with non-null names, units and kinds, and compares the result with literal rows: a labelled
    identity, a labelled MMD identity with a unit, an identity with no name, a unit that only applies to MMD, and a column with no kind
    [828].
-#}
with

identities as (
    select *
    from (
        values
            ('fct_x', 'K1', 'value_number', '2012', cast(null as varchar)),
            ('fct_mmd_prevalence', 'Condition one', 'value_number', cast(null as varchar), cast(null as varchar)),
            ('fct_x', 'K2', 'value_published', cast(null as varchar), '2020'),
            ('fct_x', 'Condition one', 'no_kind', cast(null as varchar), cast(null as varchar))
    ) as fixed (fact, measure_key, value_column, vintage_start, vintage_end)
),

names as (
    select *
    from (
        values
            ('fct_x', 'K1', 'Name of K1'),
            ('fct_mmd_prevalence', 'Condition one', 'Name of the condition')
    ) as fixed (fact, measure_key, measure_name)
),

units as (
    select *
    from (
        values
            ('Condition one', 'percent')
    ) as fixed (measure_key, value_unit)
),

value_kinds as (
    select *
    from (
        values
            ('fct_x', 'value_number', 'number'),
            ('fct_mmd_prevalence', 'value_number', 'number'),
            ('fct_x', 'value_published', 'text')
    ) as fixed (fact, value_column, value_kind)
),

actual as (
    {{ gold_measure_rows('identities', 'names', 'units', 'value_kinds') }}
),

expected as (
    select *
    from (
        values
            ('fct_x|K1|value_number', 'Name of K1', cast(null as varchar), 'number', '2012', cast(null as varchar)),
            ('fct_mmd_prevalence|Condition one|value_number', 'Name of the condition', 'percent', 'number', cast(null as varchar), cast(null as varchar)),
            ('fct_x|K2|value_published', cast(null as varchar), cast(null as varchar), 'text', cast(null as varchar), '2020'),
            ('fct_x|Condition one|no_kind', cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar))
    ) as fixed (measure_identity, measure_label, unit, value_kind, vintage_start, vintage_end)
)

select
    'row not expected' as problem,
    *
from (
    select
        measure_identity,
        measure_label,
        unit,
        value_kind,
        vintage_start,
        vintage_end
    from actual
    except all
    select * from expected
) as unexpected
union all
select
    'row missing' as problem,
    *
from (
    select * from expected
    except all
    select
        measure_identity,
        measure_label,
        unit,
        value_kind,
        vintage_start,
        vintage_end
    from actual
) as missing
{% endtest %}


{% test measure_attributes_match_silver(model, silver_models) %}
{#-
    Checks value_kind against the type of the silver column in the built database and unit against the silver value_unit of
    int_mmd_prevalence. silver_models maps a fact or bridge to the silver model that holds its columns [828].
-#}
{%- for silver_model in silver_models.values() | unique %}
    {%- do ref(silver_model) %}
{%- endfor %}
{%- do ref('int_mmd_prevalence') %}
with

fact_models as (
    select *
    from (
        values
            {%- for fact, silver_model in silver_models.items() %}
            ('{{ fact }}', '{{ silver_model }}'){% if not loop.last %},{% endif %}
            {%- endfor %}
    ) as mapped (fact, silver_model)
),

silver_types as (
    select
        table_name as silver_model,
        column_name as value_column,
        data_type
    from information_schema.columns
    where table_catalog = current_database()
),

expected_kinds as (
    select
        fact_models.fact,
        silver_types.value_column,
        case
            when silver_types.data_type in ('DOUBLE', 'INTEGER', 'BIGINT') then 'number'
            when silver_types.data_type in ('VARCHAR', 'VARCHAR[]') then 'text'
            when silver_types.data_type = 'BOOLEAN' then 'boolean'
            when silver_types.data_type = 'DATE' then 'date'
        end as value_kind
    from fact_models
    inner join silver_types
        on fact_models.silver_model = silver_types.silver_model
),

silver_units as (
    select distinct
        condition_label,
        value_unit
    from {{ ref('int_mmd_prevalence') }}
)

select
    'value_kind differs from the silver column type' as problem,
    measures.measure_identity
from {{ model }} as measures
left join expected_kinds
    on
        measures.fact = expected_kinds.fact
        and measures.value_column = expected_kinds.value_column
where measures.fact is not null and measures.value_kind is distinct from expected_kinds.value_kind
union all
select
    'unit differs from the silver value_unit' as problem,
    measures.measure_identity
from {{ model }} as measures
left join silver_units
    on
        measures.fact = 'fct_mmd_prevalence'
        and measures.measure_key = silver_units.condition_label
where measures.fact is not null and measures.unit is distinct from silver_units.value_unit
{% endtest %}


{% test bridge_matches_seeds(model, seeds) %}
{#-
    Rebuilds the expected bridge rows from the control bridge seeds (a row with a non-empty measure_key; the measure key hash of the fact,
    key and value column, and, for a split measure, the edition start, the control key hash, the control, the vintage bounds and the seed provenance) and compares them with the bridge
    in both directions with multiplicity, so a dropped, doubled or substituted mapping fails [827] [744].
-#}
with

seed_rows as (
    select
        fact,
        measure_key,
        value_column,
        measure_control,
        vintage_start,
        vintage_end,
        source_seed,
        source_row
    from (
        {%- for seed in seeds %}
        select
            fact,
            measure_key,
            value_column,
            measure_control,
            vintage_start,
            vintage_end,
            source_seed,
            source_row
        from {{ ref(seed) }}
        {%- if not loop.last %}
        union all
        {%- endif %}
        {%- endfor %}
    ) as all_seed_rows
    where measure_key is not null and measure_key <> ''
),

editions as (
    select
        fact,
        measure_key,
        value_column,
        edition_start,
        edition_end
    from {{ ref('dim_measure') }}
    where edition_start is not null
),

-- A seed row of a split measure expects one bridge row per edition whose years overlap its vintage range (every edition when the range is empty).
expected as (
    select
        md5(seed_rows.fact || '|' || seed_rows.measure_key || '|' || seed_rows.value_column || coalesce('|' || editions.edition_start, '')) as measure_sk,
        md5(seed_rows.measure_control) as registry_control_sk,
        seed_rows.measure_control,
        seed_rows.vintage_start,
        seed_rows.vintage_end,
        seed_rows.source_seed,
        seed_rows.source_row
    from seed_rows
    left join editions
        on
            seed_rows.fact = editions.fact
            and seed_rows.measure_key = editions.measure_key
            and seed_rows.value_column = editions.value_column
            and coalesce(nullif(seed_rows.vintage_start, ''), '0000') <= left(editions.edition_end, 4)
            and coalesce(nullif(seed_rows.vintage_end, ''), '9999') >= left(editions.edition_start, 4)
),

actual as (
    select
        measure_sk,
        registry_control_sk,
        measure_control,
        vintage_start,
        vintage_end,
        source_seed,
        source_row
    from {{ model }}
)

select
    'bridge row no seed row gives' as problem,
    *
from (
    select * from actual
    except all
    select * from expected
) as unexpected
union all
select
    'seed row missing from the bridge' as problem,
    *
from (
    select * from expected
    except all
    select * from actual
) as missing
{% endtest %}


{% test release_copy_consistency(model) %}
{#-
    Reports every checksum whose stored copies carry different publication_release texts. dim_release keeps the smallest object key copy
    (amendment A40 item 2); this test is severity warn in the YAML and reports the conflict without failing the build [824].
-#}
select
    member_sha256,
    count(distinct publication_release) as release_texts
from {{ ref('stg_bronze__file_copies') }}
where member_sha256 is not null
group by member_sha256
having count(distinct publication_release) > 1
{% endtest %}
