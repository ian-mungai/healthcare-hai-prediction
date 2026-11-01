{#-
    The hooks around the build marker (D1 section 10 items 4 and 5). They act only on a real build (hai_provenance_required): fixture and lint builds are not changed.
    Register them in dbt_project.yml:

        on-run-start: ["{{ hai_drop_build_marker() }}"]
        on-run-end: ["{{ hai_finalize_build_marker(results) }}"]

    dbt runs the hooks for build, run, seed, snapshot and test, the commands the launcher calls mutating (scripts/lakehouse/provenance.py HOOKED), and also
    for an unselected compile. compile and debug run no model SQL and change no relation, so the marker of the last build stays true for them: the hooks do
    nothing but check the provenance values when the command is one of hai_marker_untouched_commands (the launcher's CONNECTING and INERT commands). The case
    hook_command_coverage tries every command and form the launcher admits and requires the marker to be dropped exactly when the launcher calls it mutating.

    on-run-start checks that every provenance value is recorded (so a selected build without the marker model stops too) and drops the marker, so no marker
    survives a launch that does not rebuild it. The marker model builds the row during the run so that its tests can read it. on-run-end then WRITES the
    marker (a new table from the provenance values and this invocation) after a gold-eligible build, and drops it after any other launch. A run that
    ends before on-run-end leaves no seal: the launcher seals only a zero exit and a gold-eligible results file (scripts/lakehouse/provenance.py).
-#}
{% macro hai_drop_marker_relation() %}
    {%- do run_query('drop table if exists ' ~ adapter.quote(target.schema) ~ '.' ~ adapter.quote('int_silver_build_marker')) -%}
{% endmacro %}

{#- The commands that run no model SQL and must leave the marker as it is, also when dbt runs the hooks for them (review delta 1, finding 3). -#}
{% macro hai_marker_untouched_commands() %}
    {{ return(['compile', 'debug', 'ls', 'parse']) }}
{% endmacro %}

{% macro hai_marker_applies() %}
    {{ return(invocation_args_dict.get('which') not in hai_marker_untouched_commands()) }}
{% endmacro %}

{% macro hai_drop_build_marker() %}
    {%- if hai_provenance_required() -%}
        {%- do hai_build_provenance() -%}
        {%- if hai_marker_applies() -%}
            {%- do hai_drop_marker_relation() -%}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{#- The selection of the silver layer build (scripts/lakehouse/pipeline_build.py layer_selection): the seeds and the silver models with what they depend on.
    The build step of a layer task always passes it, so it counts as the whole layer. Any other selection, and any exclusion, is a partial build. -#}
{% macro hai_silver_layer_selection() %}
    {{ return(['resource_type:seed', '+path:models/silver']) }}
{% endmacro %}

{#- A whole build whose results are all success or pass (D1 section 10 item 5): no selection, or the selection of the silver layer build. No result may warn. -#}
{% macro hai_build_eligible(results, which, selection) %}
    {%- if which != 'build' or (selection | length > 0 and selection != hai_silver_layer_selection()) or results | length == 0 -%}
        {{ return(false) }}
    {%- endif -%}
    {%- for result in results -%}
        {%- if result.status | string | lower not in ['success', 'pass'] -%}
            {{ return(false) }}
        {%- endif -%}
    {%- endfor -%}
    {{ return(true) }}
{% endmacro %}

{% macro hai_sql_literal(value) %}
    {{ return("'" ~ (value | string | replace("'", "''")) ~ "'") }}
{% endmacro %}

{#- The marker row of this invocation, written by the end hook; the columns are those of the marker model. -#}
{% macro hai_write_build_marker() %}
    {%- set values = hai_build_provenance() -%}
    {%- do run_query(
        'create or replace table ' ~ adapter.quote(target.schema) ~ '.' ~ adapter.quote('int_silver_build_marker') ~ ' as select '
        ~ hai_sql_literal(invocation_id) ~ ' as dbt_invocation_id, '
        ~ hai_sql_literal(values.git_revision) ~ ' as git_revision, '
        ~ 'version() as duckdb_version, '
        ~ hai_sql_literal(values.run_id) ~ ' as run_id, '
        ~ hai_sql_literal(values.dbt_tree_sha256) ~ ' as dbt_tree_sha256, '
        ~ hai_sql_literal(values.bronze_pins_sha256) ~ ' as bronze_pins_sha256, '
        ~ hai_sql_literal(values.pins_json) ~ ' as pins_json'
    ) -%}
{% endmacro %}

{#- What selects part of a build, from the invocation's own arguments: the hook context's ``flags`` does not carry --select (tested in hook_command_coverage). -#}
{% macro hai_build_selection() %}
    {%- set args = invocation_args_dict -%}
    {%- set selected = (args.get('select') or []) | list -%}
    {%- set excluded = (args.get('exclude') or []) | list -%}
    {%- set typed = (args.get('resource_types') or []) | list + (args.get('exclude_resource_types') or []) | list -%}
    {%- set named = [args.get('selector')] if args.get('selector') else [] -%}
    {%- set empty = ['--empty'] if args.get('empty') else [] -%}
    {{ return(selected + excluded + typed + named + empty) }}
{% endmacro %}

{% macro hai_finalize_build_marker(results) %}
    {%- if hai_provenance_required() and hai_marker_applies() -%}
        {%- if hai_build_eligible(results, invocation_args_dict.get('which'), hai_build_selection()) -%}
            {%- do hai_write_build_marker() -%}
        {%- else -%}
            {%- do hai_drop_marker_relation() -%}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
