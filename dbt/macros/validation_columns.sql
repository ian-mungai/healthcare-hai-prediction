{% macro validation_tokens() %}
{#- The published marks for a Care Compare validation value that is not given [510]. -#}
('Not Available', 'Not Applicable', 'N/A', 'Too Few to Report')
{% endmacro %}

{% macro validation_reviewed_values() %}
{#- Published values reviewed by the owner that stay text with no number (Oct 8 2026) [519]. -#}
('24.083333333333(23)')
{% endmacro %}
