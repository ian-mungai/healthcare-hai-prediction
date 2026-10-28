{#-
    The provenance values of a build. The launcher puts them in the environment of every real_dbt launch
    (scripts/lakehouse/provenance.py); a caller cannot set them. On a real build a missing or `unrecorded` value stops compilation. Fixture and lint
    builds are not required to carry them and read `unrecorded` for each value.
-#}
{% macro hai_provenance_required() %}
    {{ return(target.name == 'lakehouse' or env_var('HAI_PROVENANCE_REQUIRED', '') == '1') }}
{% endmacro %}

{% macro hai_build_provenance() %}
    {%- set names = {
        'run_id': 'HAI_BUILD_RUN_ID',
        'dbt_tree_sha256': 'HAI_BUILD_DBT_TREE_SHA256',
        'bronze_pins_sha256': 'HAI_BUILD_PINS_SHA256',
        'pins_json': 'HAI_BUILD_PINS_JSON',
        'git_revision': 'HAI_BUILD_GIT_REVISION',
    } -%}
    {%- set patterns = {
        'run_id': '[A-Za-z0-9][A-Za-z0-9_.-]{0,63}',
        'dbt_tree_sha256': '[0-9a-f]{64}',
        'bronze_pins_sha256': '[0-9a-f]{64}',
        'git_revision': '[0-9a-f]{40}|[0-9a-f]{64}',
    } -%}
    {%- set required = hai_provenance_required() -%}
    {%- set values = {} -%}
    {%- for column, name in names.items() -%}
        {%- set value = env_var(name, '') -%}
        {%- if required -%}
            {%- if value == '' or value == 'unrecorded' -%}
                {{ exceptions.raise_compiler_error('build provenance: ' ~ name ~ ' is missing or unrecorded; a real build needs every provenance value') }}
            {%- endif -%}
            {%- if column in patterns and not modules.re.fullmatch(patterns[column], value) -%}
                {{ exceptions.raise_compiler_error('build provenance: ' ~ name ~ ' is not a valid ' ~ column) }}
            {%- endif -%}
            {%- if column == 'pins_json' -%}
                {%- set pins = fromjson(value, none) -%}
                {%- if pins is none or pins is not mapping or pins | length == 0 -%}
                    {{ exceptions.raise_compiler_error('build provenance: ' ~ name ~ ' is not a JSON object of table snapshot IDs') }}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
        {%- if value == '' -%}
            {%- set value = var('git_revision', 'unrecorded') if column == 'git_revision' else 'unrecorded' -%}
        {%- endif -%}
        {%- do values.update({column: value}) -%}
    {%- endfor -%}
    {{ return(values) }}
{% endmacro %}
