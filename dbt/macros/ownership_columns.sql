{% macro owner_flag_columns() %}
{#- The owner type flags, as published (Y, N or blank). The last four start with the April 2025 layout. -#}
{{ return([
    'created_for_acquisition_owner',
    'corporation_owner',
    'llc_owner',
    'medical_provider_supplier_owner',
    'management_services_company_owner',
    'medical_staffing_company_owner',
    'holding_company_owner',
    'investment_firm_owner',
    'financial_institution_owner',
    'consulting_firm_owner',
    'for_profit_owner',
    'non_profit_owner',
    'other_type_owner',
    'private_equity_company_owner',
    'reit_owner',
    'chain_home_office_owner',
    'owned_by_another_org_or_ind_owner'
]) }}
{% endmacro %}

{% macro enrollment_flag_columns() %}
{#- The enrollment flags, as published (Y, N or blank). -#}
{{ return([
    'multiple_npi_flag',
    'subgroup_general',
    'subgroup_acute_care',
    'subgroup_alcohol_drug',
    'subgroup_childrens',
    'subgroup_long_term',
    'subgroup_psychiatric',
    'subgroup_rehabilitation',
    'subgroup_short_term',
    'subgroup_swing_bed_approved',
    'subgroup_psychiatric_unit',
    'subgroup_rehabilitation_unit',
    'subgroup_specialty_hospital',
    'subgroup_other',
    'reh_conversion_flag'
]) }}
{% endmacro %}

{% macro yes_no(column) %}
{#- Y is true and N is false; blank, null and anything else are null [366]. -#}
case upper(trim({{ column }})) when 'Y' then true when 'N' then false end
{%- endmacro %}

{% macro month_day_year(column) %}
{#- A date written M/D/YYYY, with one or two digits for month and day; anything else is null [369]. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
case when regexp_full_match({{ value }}, '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}') then try_strptime({{ value }}, '%m/%d/%Y')::date end
{%- endmacro %}

{% macro published_ccn(column) %}
{#- A CCN of 6 characters as published, or a 5-digit CCN that lost its leading zero padded to 6; anything else is null [370]. -#}
{%- set value = "upper(nullif(trim(" ~ column ~ "), ''))" -%}
case
    when regexp_full_match({{ value }}, '[0-9]{5}') then lpad({{ value }}, 6, '0')
    when regexp_full_match({{ value }}, '[0-9A-Z]{6}') then {{ value }}
end
{%- endmacro %}
