{% macro file_metadata_source() %}
    {%- set pins = fromjson(env_var('HAI_BUILD_PINS_JSON', '{}')) -%}
    {%- set snapshot = pins['file_metadata'] if 'file_metadata' in pins else none -%}
    {%- if (target.name == 'lakehouse' or env_var('HAI_PROVENANCE_REQUIRED', '0') == '1') and snapshot is none -%}
        {{ exceptions.raise_compiler_error('The build must pin bronze.file_metadata before reading release observations or the object index.') }}
    {%- endif -%}
    {{ source('bronze', 'file_metadata') }}
    {%- if target.name == 'lakehouse' and snapshot is not none %} at (version => {{ snapshot | int }}){% endif -%}
{% endmacro %}

{% macro assert_original_label_keys() %}
select case
    when count(*) = 0 then true
    else error('one original object key maps to conflicting IPPS label classifications')
end as labels_are_unambiguous
from (
    select bronze_table, object_key
    from (select distinct * from {{ ref('ipps_occmix_copy_labels') }}) as labels
    group by bronze_table, object_key
    having count(*) > 1
) as conflicts
{% endmacro %}

{% macro assert_file_metadata_generation() %}
select case
    when count(*) > 0
        and count(distinct generation) = 1
        and count(*) = count(generation)
        and count(*) = count(kind)
        and count(*) filter (where retired is null or failed_attempt is null) = 0
        and count(*) filter (where kind not in ('object', 'observation')) = 0
        and (
            count(*) filter (where kind = 'object') > 0
            or count(*) filter (where kind = 'observation' and not retired and not failed_attempt) = 0
        )
        and count(*) filter (where kind = 'observation') > 0
        and count(*) = count(generation_row_count)
        and count(*) = count(generation_object_count)
        and count(*) = min(generation_row_count)
        and count(*) = max(generation_row_count)
        and count(*) filter (where kind = 'object') = min(generation_object_count)
        and count(*) filter (where kind = 'object') = max(generation_object_count)
        then true
    else error('bronze.file_metadata is an incomplete generation or is missing a metadata kind')
end as is_complete
from {{ file_metadata_source() }}
{% endmacro %}
