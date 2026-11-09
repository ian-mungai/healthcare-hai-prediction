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

{% macro sm7_category(column) %}
{#- The category of a published SM_7 text (C167): Yes, No and the Not Applicable text of a hospital without inpatient labor and delivery care are categories.
    Not Available and any other text give null, and a test fails text outside sm7_published_texts. -#}
case trim({{ column }})
    when 'Yes' then 'yes'
    when 'No' then 'no'
    when 'Not Applicable (our hospital does not provide inpatient labor/delivery care)' then 'not applicable'
end
{% endmacro %}

{% macro sm7_published_texts() %}
{#- Every SM_7 text (C167, the maternal morbidity structural measure) staging may carry: Yes, No, the Not Applicable text of a hospital without inpatient
    labor and delivery care, and the Not Available token. -#}
('Yes', 'No', 'Not Applicable (our hospital does not provide inpatient labor/delivery care)', 'Not Available')
{% endmacro %}
