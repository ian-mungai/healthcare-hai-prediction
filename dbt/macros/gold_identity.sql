{% macro gold_measure_control_rows(seed_rows, editions) %}
{#-
    The rows of bridge_measure_control from the control bridge seed rows and the editions of the split measures: a seed row of a measure with no
    editions keeps the key of its identity (fact, measure key and value column); a seed row of a split measure maps to every edition when its
    vintage range is empty and to the editions whose years overlap the range otherwise. editions holds one row per edition of a split identity
    (fact, measure_key, value_column, edition_start, edition_end). The test bridge_edition_branches feeds it fixture rows.
-#}
select
    seed_rows.measure_control,
    seed_rows.vintage_start,
    seed_rows.vintage_end,
    seed_rows.source_seed,
    seed_rows.source_row,
    md5(concat_ws('|', seed_rows.fact, seed_rows.measure_key, seed_rows.value_column, editions.edition_start)) as measure_sk,
    md5(seed_rows.measure_control) as registry_control_sk
from {{ seed_rows }} as seed_rows
left join {{ editions }} as editions
    on
        seed_rows.fact = editions.fact
        and seed_rows.measure_key = editions.measure_key
        and seed_rows.value_column = editions.value_column
        and (seed_rows.vintage_start is null or seed_rows.vintage_start = '' or left(editions.edition_end, 4) >= seed_rows.vintage_start)
        and (seed_rows.vintage_end is null or seed_rows.vintage_end = '' or left(editions.edition_start, 4) <= seed_rows.vintage_end)
{% endmacro %}


{% test bridge_edition_branches(model) %}
{#-
    Runs gold_measure_control_rows over fixture rows and compares the result with literal rows: a measure with no editions, a split measure with an
    empty range (every edition), a range that overlaps the first edition only, a range that overlaps both and a range that overlaps neither (the
    identity key without an edition, which the bridge's relationship test then refuses).
-#}
with

seed_rows as (
    select *
    from (
        values
            ('fct_x', 'PLAIN', 'score', cast(null as varchar), cast(null as varchar), 'C1', 'seed_one', 1),
            ('fct_x', 'SPLIT', 'score', cast(null as varchar), cast(null as varchar), 'C2', 'seed_one', 2),
            ('fct_x', 'SPLIT', 'score', '2020', '2021', 'C3', 'seed_one', 3),
            ('fct_x', 'SPLIT', 'score', '2020', '2024', 'C4', 'seed_one', 4),
            ('fct_x', 'SPLIT', 'score', '2030', cast(null as varchar), 'C5', 'seed_one', 5),
            ('fct_x', 'SPLIT', 'other', cast(null as varchar), cast(null as varchar), 'C6', 'seed_one', 6)
    ) as fixed (fact, measure_key, value_column, vintage_start, vintage_end, measure_control, source_seed, source_row)
),

editions as (
    select *
    from (
        values
            ('fct_x', 'SPLIT', 'score', '2019-05-01', '2022-08-01'),
            ('fct_x', 'SPLIT', 'score', '2023-02-01', '2025-01-01')
    ) as fixed (fact, measure_key, value_column, edition_start, edition_end)
),

actual as (
    {{ gold_measure_control_rows('seed_rows', 'editions') }}
),

expected as (
    select *
    from (
        values
            ('C1', cast(null as varchar), cast(null as varchar), md5('fct_x|PLAIN|score')),
            ('C2', cast(null as varchar), cast(null as varchar), md5('fct_x|SPLIT|score|2019-05-01')),
            ('C2', cast(null as varchar), cast(null as varchar), md5('fct_x|SPLIT|score|2023-02-01')),
            ('C3', '2020', '2021', md5('fct_x|SPLIT|score|2019-05-01')),
            ('C4', '2020', '2024', md5('fct_x|SPLIT|score|2019-05-01')),
            ('C4', '2020', '2024', md5('fct_x|SPLIT|score|2023-02-01')),
            ('C5', '2030', cast(null as varchar), md5('fct_x|SPLIT|score')),
            ('C6', cast(null as varchar), cast(null as varchar), md5('fct_x|SPLIT|other'))
    ) as fixed (measure_control, vintage_start, vintage_end, measure_sk)
)

select
    'row not expected' as problem,
    *
from (
    select
        measure_control,
        vintage_start,
        vintage_end,
        measure_sk
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
        measure_control,
        vintage_start,
        vintage_end,
        measure_sk
    from actual
) as missing
{% endtest %}


{% macro gold_label_sources() %}
{#-
    The silver sources of the three dimensions that carry a label which can change across releases, as the tests of the identity rules read them: the
    dimension, the code system of its seed rows, the gold fact of a measure (empty for a code), the silver model, its key and label columns and its
    two ordering columns (a null second column when there is none). It lists the same sources as dim_drg, dim_rurality_code and dim_measure, written
    out again so that a test fails when a model drops or adds one.
-#}
{{ return([
    {'dimension': 'dim_drg', 'code_system': 'int_mup_drg_discharges.drg_cd', 'fact': '', 'model': 'int_mup_drg_discharges',
     'code': 'drg_cd', 'label': 'drg_desc', 'axis': 'data_year', 'axis_2': 'null'},
    {'dimension': 'dim_rurality_code', 'code_system': 'int_rucc_county_codes.rucc_code', 'fact': '', 'model': 'int_rucc_county_codes',
     'code': 'rucc_code', 'label': 'description', 'axis': 'vintage', 'axis_2': 'null'},
    {'dimension': 'dim_measure', 'code_system': 'int_cc_complications_deaths_windows.measure_id', 'fact': 'fct_complications_deaths_window',
     'model': 'int_cc_complications_deaths_windows', 'code': 'measure_id', 'label': 'measure_name', 'axis': 'release_date', 'axis_2': 'window_end'},
    {'dimension': 'dim_measure', 'code_system': 'int_hai_hospital_windows.measure_id', 'fact': 'fct_hai_hospital_window',
     'model': 'int_hai_hospital_windows', 'code': 'measure_id', 'label': 'measure_name', 'axis': 'release_date', 'axis_2': 'window_end'},
    {'dimension': 'dim_measure', 'code_system': 'int_hai_national_windows.measure_id', 'fact': 'fct_hai_national_window',
     'model': 'int_hai_national_windows', 'code': 'measure_id', 'label': 'measure_name', 'axis': 'release_date', 'axis_2': 'window_end'},
    {'dimension': 'dim_measure', 'code_system': 'int_hai_state_windows.measure_id', 'fact': 'fct_hai_state_window',
     'model': 'int_hai_state_windows', 'code': 'measure_id', 'label': 'measure_name', 'axis': 'release_date', 'axis_2': 'window_end'},
    {'dimension': 'dim_measure', 'code_system': 'int_cc_hcahps_windows.measure_id', 'fact': 'fct_hcahps_window',
     'model': 'int_cc_hcahps_windows', 'code': 'measure_id', 'label': 'measure_name', 'axis': 'release_date', 'axis_2': 'window_end'},
    {'dimension': 'dim_measure', 'code_system': 'int_cc_maternal_windows.measure_id', 'fact': 'fct_maternal_health_window',
     'model': 'int_cc_maternal_windows', 'code': 'measure_id', 'label': 'measure_name', 'axis': 'release_date', 'axis_2': 'window_end'},
    {'dimension': 'dim_measure', 'code_system': 'int_cc_timely_effective_windows.measure_id', 'fact': 'fct_timely_effective_care_window',
     'model': 'int_cc_timely_effective_windows', 'code': 'measure_id', 'label': 'measure_name', 'axis': 'release_date', 'axis_2': 'window_end'},
    {'dimension': 'dim_measure', 'code_system': 'int_cc_unplanned_visits_windows.measure_id', 'fact': 'fct_unplanned_visits_window',
     'model': 'int_cc_unplanned_visits_windows', 'code': 'measure_id', 'label': 'measure_name', 'axis': 'release_date', 'axis_2': 'window_end'},
]) }}
{% endmacro %}


{% macro gold_silver_label_rows() %}
{#-
    One row per silver row that has a key and a label, from every source of gold_label_sources: dimension, code_system, fact, code, published_label and the
    two ordering values as text. The tests of the identity rules select from it.
-#}
{%- for source in gold_label_sources() %}
select
    '{{ source.dimension }}' as dimension,
    '{{ source.code_system }}' as code_system,
    '{{ source.fact }}' as fact,
    cast({{ source.code }} as varchar) as code,
    cast({{ source.label }} as varchar) as published_label,
    cast({{ source.axis }} as varchar) as axis,
    cast({{ source.axis_2 }} as varchar) as axis_2
from {{ ref(source.model) }}
where {{ source.code }} is not null and {{ source.label }} is not null
{%- if not loop.last %}
union all
{%- endif %}
{%- endfor %}
{% endmacro %}
