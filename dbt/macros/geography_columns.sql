{% macro county_fips(column) %}
{#- A county code as 5-character text: 5 digits as published, 4 digits left-padded (a lost leading zero); anything else is
    null [400]. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
case
    when regexp_full_match({{ value }}, '[0-9]{5}') then {{ value }}
    when regexp_full_match({{ value }}, '[0-9]{4}') then '0' || {{ value }}
end
{%- endmacro %}

{% macro state_fips_and_dc() %}
{#- The FIPS codes of the 50 states and DC, the approved geographic universe (Oct 1 2026). -#}
('01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24',
    '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46',
    '47', '48', '49', '50', '51', '53', '54', '55', '56')
{% endmacro %}

{% macro county_scope(fips) %}
{#- state for a county of the 50 states and DC, territory for the island areas, not_county for anything else [401]. -#}
case
    when left({{ fips }}, 2) in {{ state_fips_and_dc() }} then 'state'
    when left({{ fips }}, 2) in ('60', '64', '66', '68', '69', '70', '72', '74', '78') then 'territory'
    else 'not_county'
end
{%- endmacro %}

{% macro zip_code(column) %}
{#- A ZIP code as 5-character text after removing the apostrophes that wrap it (the 2010 RUCA file publishes ''00501'');
    anything else is null [407]. -#}
{%- set value = "regexp_replace(trim(" ~ column ~ "), '^''+([^'']*)''+$', '\\1')" -%}
case when regexp_full_match({{ value }}, '[0-9]{5}') then {{ value }} end
{%- endmacro %}

{% macro category_code(column) %}
{#- A published category code as text: trimmed, a workbook's trailing .0 removed, blank null [411]. -#}
nullif(regexp_replace(trim({{ column }}), '\.0$', ''), '')
{%- endmacro %}

{% macro ruca_secondary_codes() %}
{#- The 22 published secondary RUCA codes, 99 included [411]. -#}
('1', '1.1', '2', '2.1', '3', '4', '4.1', '5', '5.1', '6', '7', '7.1', '7.2', '8', '8.1', '8.2', '9', '10', '10.1', '10.2', '10.3', '99')
{% endmacro %}

{% macro unquoted(column) %}
{#- A text field with one pair of wrapping double quotes removed; blank is null. -#}
nullif(regexp_replace(trim({{ column }}), '^"(.*)"$', '\1'), '')
{%- endmacro %}
