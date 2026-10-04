{% macro clean_field(field) -%}
{#- A field trimmed, with surrounding double quotes removed and doubled quotes unescaped, then trimmed again: spaces
    inside quotes are not data [184] [200]. -#}
trim(replace(regexp_replace(trim(coalesce({{ field }}, '')), '^"(.*)"$', '\1'), '""', '"'))
{%- endmacro %}

{% macro plain_number(field) -%}
{#- The field's text without $ and thousands separators, as a text twin displays numbers [189]. -#}
replace(replace({{ clean_field(field) }}, '$', ''), ',', '')
{%- endmacro %}

{% macro field_number(field) -%}
{#- The field as a number; a percentage is divided by 100 [191]. -#}
case
    when ends_with({{ plain_number(field) }}, '%') then try_cast(rtrim({{ plain_number(field) }}, '%') as double) / 100
    else try_cast({{ plain_number(field) }} as double)
end
{%- endmacro %}

{% macro field_decimals(field) -%}
{#- Decimals the text displays; two more for a percentage [191]. -#}
length(regexp_extract(rtrim({{ plain_number(field) }}, '%'), '\.(\d+)$', 1))
    + case when ends_with({{ plain_number(field) }}, '%') then 2 else 0 end
{%- endmacro %}

{% macro field_matches(text_field, workbook_field) -%}
{#- A text number with d decimals matches a workbook number within half a unit in the d-th decimal, plus 1e-13 of the
    workbook value for floating-point error [189] [192]; other fields must be equal once cleaned. -#}
case
    when {{ field_number(text_field) }} is not null and {{ field_number(workbook_field) }} is not null
        then abs({{ field_number(text_field) }} - {{ field_number(workbook_field) }})
            <= 0.5 * pow(10, -({{ field_decimals(text_field) }})) + 1e-9 + abs({{ field_number(workbook_field) }}) * 1e-13
    -- A text date (m/d/yyyy) matches the same date in the workbook, as an ISO value or an Excel serial number [202].
    when {{ text_date(text_field) }} is not null and {{ workbook_date(workbook_field) }} is not null
        then {{ text_date(text_field) }} = {{ workbook_date(workbook_field) }}
    else {{ clean_field(text_field) }} = {{ clean_field(workbook_field) }}
end
{%- endmacro %}

{% macro text_date(field) -%}
try_strptime({{ clean_field(field) }}, '%m/%d/%Y')::date
{%- endmacro %}

{% macro workbook_date(field) -%}
{#- An Excel serial number counts days after Dec 30 1899 (the 1900 date system); otherwise an ISO date or timestamp. -#}
case
    when try_cast({{ clean_field(field) }} as double) between 1 and 2958465
        then date '1899-12-30' + cast(floor(try_cast({{ clean_field(field) }} as double)) as integer)
    else try_cast({{ clean_field(field) }} as timestamp)::date
end
{%- endmacro %}

{% macro fields_match(text_fields, workbook_fields) -%}
{#- True when every position matches; a missing trailing field reads as empty. -#}
list_bool_and(list_transform(
    range(1, greatest(len({{ text_fields }}), len({{ workbook_fields }})) + 1),
    lambda i: {{ field_matches(text_fields ~ '[i]', workbook_fields ~ '[i]') }}
))
{%- endmacro %}

{% macro row_has_number(fields) -%}
{#- True when any field is a number: such a row is a data row [185]. -#}
coalesce(list_bool_or(list_transform({{ fields }}, lambda f: {{ field_number('f') }} is not null)), false)
{%- endmacro %}
