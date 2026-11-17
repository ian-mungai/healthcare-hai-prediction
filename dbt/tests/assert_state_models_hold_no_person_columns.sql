-- Fails for each column of a state-source silver model that is a withheld person, phone or address column of the HCAI utilization files, or whose name
-- says so. Bronze redacts these cells; silver never reads them.
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
    schema_columns.table_name in ('int_hcai_finance_values', 'int_hcai_util_facility_periods', 'int_illinois_report_card_values', 'int_state_source_holds')
    and (
        withheld.column_name is not null
        or schema_columns.column_name like '%phone%'
        or schema_columns.column_name like '%admin%'
        or schema_columns.column_name like '%prep_name%'
        or schema_columns.column_name like '%street%'
    )
