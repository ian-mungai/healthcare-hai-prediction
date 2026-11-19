-- Fails for each row of a published state-source table (staging views and held rows included) that contains the text SENTINEL_, which the fixture plants
-- in every person, phone and address cell and in every rejected identifier, date, county, number and free text. The fixture target only: a real build
-- has no sentinel.
{%- if target.name != 'fixture' %}
select null as table_name
where false
{%- else %}
with

leaked as (
    select
        'stg_hcai_util_pages_csv' as table_name,
        t::varchar as row_text
    from {{ ref('stg_hcai_util_pages_csv') }} as t
    union all
    select
        'stg_hcai_util_nonresponse_csv' as table_name,
        t::varchar as row_text
    from {{ ref('stg_hcai_util_nonresponse_csv') }} as t
    union all
    select
        'stg_illinois_report_card_csv' as table_name,
        t::varchar as row_text
    from {{ ref('stg_illinois_report_card_csv') }} as t
    union all
    select
        'int_hcai_util_facility_periods' as table_name,
        t::varchar as row_text
    from {{ ref('int_hcai_util_facility_periods') }} as t
    union all
    select
        'int_illinois_report_card_values' as table_name,
        t::varchar as row_text
    from {{ ref('int_illinois_report_card_values') }} as t
    union all
    select
        'int_state_source_holds' as table_name,
        t::varchar as row_text
    from {{ ref('int_state_source_holds') }} as t
)

select table_name
from leaked
where row_text like '%SENTINEL\_%' escape '\'
{%- endif %}
