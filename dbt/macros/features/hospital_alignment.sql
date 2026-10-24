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
