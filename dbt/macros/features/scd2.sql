{% macro scd2_versions(rows, releases, keys, tracked, bridge=0, held=none) %}
{#- Type 2 versions from source-dated releases (silver step 7.3, failure modes 670 to 674, 714 and 715).
    rows: a relation with the key columns, release_date, release_number, member_sha256 and the tracked columns, already
    one row per key and release. releases: every release_date with its release_number, in order, so a key missing from a
    release is a gap. A version starts at a key's first release, after a gap or when a tracked column changes,
    compared null-safely; it ends (exclusive) at the release after its last one, or stays open at the latest release.
    bridge: the longest gap, in releases, that does not count as a gap; the version covers it, or extends to the next
    version when the tracked columns change on return, and bridged_releases counts it (a fill, so marked [715]).
    held: a relation with the key columns and release_number of releases held for disagreeing rows [674]; a gap that
    contains one is never bridged. With bridge 0 the output has no bridged_releases column. -#}
{%- set key_list = keys | join(', ') -%}
compared as (
    select
        *,
        release_number - lag(release_number) over by_key - 1 as gap_before,
        lead(release_number) over by_key - release_number - 1 as gap_after,
        (
            {%- for column in tracked %}
            {{ 'or ' if not loop.first }}lag({{ column }}) over by_key is distinct from {{ column }}
            {%- endfor %}
        ) as attributes_changed
    from {{ rows }}
    window by_key as (partition by {{ key_list }} order by release_number)
),

{%- if bridge > 0 %}

bridgeable as (
    -- A gap of at most `bridge` releases with no held release inside it [715].
    select
        compared.*,
        coalesce(compared.gap_before between 1 and {{ bridge }}, false) and not exists (
            select 1 from {{ held }} as held
            where
                {%- for key in keys %}
                held.{{ key }} = compared.{{ key }} and
                {%- endfor %}
                held.release_number between compared.release_number - compared.gap_before and compared.release_number - 1
        ) as is_bridged_before,
        coalesce(compared.gap_after between 1 and {{ bridge }}, false) and not exists (
            select 1 from {{ held }} as held
            where
                {%- for key in keys %}
                held.{{ key }} = compared.{{ key }} and
                {%- endfor %}
                held.release_number between compared.release_number + 1 and compared.release_number + compared.gap_after
        ) as is_bridged_after
    from compared
),
{%- else %}

bridgeable as (
    select
        *,
        false as is_bridged_before,
        false as is_bridged_after
    from compared
),
{%- endif %}

starts as (
    select
        *,
        coalesce(gap_before > 0 and not is_bridged_before, false) as is_after_gap,
        gap_before is null or (gap_before > 0 and not is_bridged_before) or attributes_changed as is_version_start,
        case when is_bridged_after then gap_after else 0 end as bridged_after
    from bridgeable
),

numbered as (
    select
        *,
        -- sum() gives HUGEINT, which Iceberg cannot store, so the counters are cast to BIGINT.
        sum(case when is_version_start then 1 else 0 end) over (partition by {{ key_list }} order by release_number)::bigint as version_number
    from starts
),

versions as (
    select
        {{ key_list }},
        version_number,
        min(release_date) as valid_from,
        max(release_number + bridged_after) as last_release_number,
        count(*) as release_count,
        {%- if bridge > 0 %}
        sum(bridged_after)::bigint as bridged_releases,
        {%- endif %}
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
