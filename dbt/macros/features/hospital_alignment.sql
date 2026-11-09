{% macro ipps_stage_rank(stage) %}
{#- The IPPS rule stages, most final first: a later correction of a final rule outranks the final rule, which outranks
    every proposed or notice file; an unlabelled file comes last [579]. -#}
case {{ stage }}
    when 'correction+interim+final' then 1
    when 'correction+final' then 2
    when 'interim+final' then 3
    when 'final' then 4
    when 'correction+notice' then 5
    when 'correction' then 6
    when 'notice' then 7
    when 'proposed+notice' then 8
    when 'proposed' then 9
    else 10
end
{% endmacro %}

{% macro ownership_class(column) %}
{#- The ownership class of a POS control type code (design 2.11): 01 to 03 nonprofit, 04 and 09 for profit, 05 to 08 and 10 government; any other code or a
    missing code gives null. assert_ownership_class_codes runs every code through this macro. -#}
case
    when {{ column }} in ('01', '02', '03') then 'nonprofit'
    when {{ column }} in ('04', '09') then 'for_profit'
    when {{ column }} in ('05', '06', '07', '08', '10') then 'government'
end
{% endmacro %}
