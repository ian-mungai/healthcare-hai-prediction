{% macro edv_category(column) %}
{#- Each published spelling of emergency department volume (C141) and its one category; Not Available and any other text
    give null, and a test fails text outside this list (STG-010) [569]. -#}
case trim({{ column }})
    when 'low' then 'low'
    when 'Low' then 'low'
    when 'medium' then 'medium'
    when 'Medium' then 'medium'
    when 'high' then 'high'
    when 'High' then 'high'
    when 'very high' then 'very high'
    when 'Very High' then 'very high'
end
{% endmacro %}

{% macro edv_published_texts() %}
{#- Every C141 text staging may carry: the eight spellings above and the Not Available token [569]. -#}
('low', 'Low', 'medium', 'Medium', 'high', 'High', 'very high', 'Very High', 'Not Available')
{% endmacro %}
