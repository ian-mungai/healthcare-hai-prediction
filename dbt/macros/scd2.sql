{% macro scd2_versions(rows, releases, keys, tracked) %}
{#- Type 2 versions from source-dated releases (silver step 7.3, failure modes 670 to 674).
    rows: a relation with the key columns, release_date, release_number, member_sha256 and the tracked columns, already
    one row per key and release. releases: every release_date with its release_number, in order, so a key missing from a
    release is a gap. A version starts at a key's first release, after a gap or when a tracked column changes,
    compared null-safely; it ends (exclusive) at the release after its last one, or stays open at the latest release. -#}
{%- set key_list = keys | join(', ') -%}
compared as (
    select
        *,
        lag(release_number) over by_key as previous_number,
        (
            {%- for column in tracked %}
            {{ 'or ' if not loop.first }}lag({{ column }}) over by_key is distinct from {{ column }}
            {%- endfor %}
        ) as attributes_changed
    from {{ rows }}
    window by_key as (partition by {{ key_list }} order by release_number)
),

starts as (
    select
        *,
        previous_number is not null and previous_number < release_number - 1 as is_after_gap,
        previous_number is null or previous_number < release_number - 1 or attributes_changed as is_version_start
    from compared
),

numbered as (
    select
        *,
        sum(case when is_version_start then 1 else 0 end) over (partition by {{ key_list }} order by release_number) as version_number
    from starts
),

versions as (
    select
        {{ key_list }},
        version_number,
        min(release_date) as valid_from,
        max(release_number) as last_release_number,
        count(*) as release_count,
        arg_min(member_sha256, release_number) as first_member_sha256,
        bool_or(is_after_gap and is_version_start) as is_after_gap,
        {%- for column in tracked %}
        arg_min({{ column }}, release_number) as {{ column }}{{ ',' if not loop.last }}
        {%- endfor %}
    from numbered
    group by {{ key_list }}, version_number
),

dated_versions as (
    select
        versions.* exclude (last_release_number),
        next_release.release_date as valid_to,
        next_release.release_date is null as is_current
    from versions
    left join {{ releases }} as next_release on versions.last_release_number + 1 = next_release.release_number
)
{%- endmacro %}
