-- Fails for each column of a published state-source table (staging views included) that is a withheld person, phone or address column of the HCAI
-- utilization files, or whose name says so, that is a pseudonym column, and for each column of the held rows that could carry rejected text. Bronze
-- redacts the person cells at load; silver never reads them and publishes no pseudonym.
with

withheld as (
    {%- for item in hcai_util_withheld_columns() %}
    select '{{ item }}' as column_name
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
)

select
    schema_columns.table_name,
    schema_columns.column_name
from information_schema.columns as schema_columns
left join withheld on schema_columns.column_name = withheld.column_name
where
    schema_columns.table_name in (
        'stg_hcai_finance_csv',
        'stg_hcai_util_pages_csv',
        'stg_hcai_util_nonresponse_csv',
        'stg_illinois_report_card_csv',
        'int_hcai_finance_values',
        'int_hcai_util_facility_periods',
        'int_illinois_report_card_values',
        'int_state_source_holds'
    )
    and (
        withheld.column_name is not null
        or schema_columns.column_name like '%phone%'
        or schema_columns.column_name like '%admin%'
        or schema_columns.column_name like '%prep_name%'
        or schema_columns.column_name like '%street%'
        or schema_columns.column_name like '%pseudonym%'
        or (schema_columns.table_name = 'int_state_source_holds' and schema_columns.column_name like '%published%')
        or (schema_columns.table_name = 'stg_hcai_finance_csv' and schema_columns.column_name in ('values', 'u_id'))
    )
