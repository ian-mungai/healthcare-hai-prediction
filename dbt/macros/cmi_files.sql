{% macro cmi_snapshot_files() %}
{#- Data files of the CMI snapshots, with each distinct label of their copies and whether staging selects them [293] [299] [300]. -#}
select distinct
    labels.bronze_table,
    labels.member_sha256,
    labels.family,
    labels.rule_fiscal_year,
    labels.data_fiscal_year,
    labels.rule_stage,
    selection.is_selected
from {{ ref('stg_bronze__file_labels') }} as labels
inner join {{ ref('stg_bronze__file_copies') }} as copies on labels.copy_key = copies.copy_key
inner join {{ ref('stg_bronze__file_selection') }} as selection
    on
        labels.bronze_table = selection.bronze_table
        and labels.member_sha256 = selection.member_sha256
where
    labels.role = 'data'
    and starts_with(copies.snapshot_id, 'main-cmi-ipps')
{% endmacro %}

{% macro quoted_list(name) %}
{#- A list variable as a quoted SQL list. -#}
({% for item in var(name) %}'{{ item }}'{% if not loop.last %}, {% endif %}{% endfor %})
{% endmacro %}
