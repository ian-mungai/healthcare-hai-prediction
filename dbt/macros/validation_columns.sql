{% macro validation_tokens() %}
{#- The published marks for a Care Compare validation value that is not given [510]. -#}
('Not Available', 'Not Applicable', 'N/A', 'Too Few to Report')
{% endmacro %}

{% macro validation_reviewed_values() %}
{#- Published values reviewed by the owner that stay text with no number [519]. -#}
('24.083333333333(23)')
{% endmacro %}

{% macro validation_ccn(value) %}
{#- The hospital ID as a 6-character CCN: a 5-digit ID is padded; any other ID that is not 6 digits or capital letters is
    null, so the row is held as no_key and the published ID stays in the staging view (group B rule 370) [524]. -#}
case
    when regexp_full_match(trim({{ value }}), '[0-9]{5}') then lpad(trim({{ value }}), 6, '0')
    when regexp_full_match(trim({{ value }}), '[0-9A-Z]{6}') then trim({{ value }})
end
{% endmacro %}
