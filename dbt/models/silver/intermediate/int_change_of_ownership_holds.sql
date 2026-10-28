-- One row per change-of-ownership event and release that int_change_of_ownership_rows does not keep: differs_from_latest_release for another release
-- whose published values differ from the kept row, with the
-- differing fields; repeated_in_release for another row of the event in the kept row's release. Values are compared as
-- published, through each row's lineage (member_sha256, source_row_number) to the staging row, so a correction that the
-- cleansing hides (associate ID 123 against 0123) is still held. Each hold keeps the sorted checksums of its files and a
-- null-safe hold_key. A release with the same published values as the kept row is not held.
{{ config(materialized='table') }}

{%- set published_columns = [
    'enrollment_id_buyer',
    'enrollment_state_buyer',
    'provider_type_code_buyer',
    'provider_type_text_buyer',
    'npi_buyer',
    'multiple_npi_flag_buyer',
    'ccn_buyer',
    'associate_id_buyer',
    'organization_name_buyer',
    'doing_business_as_name_buyer',
    'chow_type_code',
    'chow_type_text',
    'effective_date',
    'enrollment_id_seller',
    'enrollment_state_seller',
    'provider_type_code_seller',
    'provider_type_text_seller',
    'npi_seller',
    'multiple_npi_flag_seller',
    'ccn_seller',
    'associate_id_seller',
    'organization_name_seller',
    'doing_business_as_name_seller'
] %}

with

release_rows as (
    {{ change_of_ownership_release_rows() }}
),

published as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        {%- for column in published_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref('stg_cms_change_of_ownership') }}
),

ranked as (
    select
        event_key,
        release_id,
        member_sha256,
        source_row_number,
        row_number() over (
            partition by event_key
            order by period_end desc nulls last, release_id desc nulls last, source_row_number asc, member_sha256 asc
        ) as event_rank
    from release_rows
),

lineage as (
    select
        ranked.event_key,
        ranked.release_id,
        ranked.member_sha256,
        ranked.event_rank,
        {%- for column in published_columns %}
        published.{{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from ranked
    inner join published
        on
            ranked.member_sha256 = published.member_sha256
            and ranked.source_row_number = published.source_row_number
),

kept as (
    select * from lineage
    where event_rank = 1
),

compared as (
    select
        listed.event_key,
        listed.release_id,
        listed.member_sha256,
        (listed.release_id is not distinct from kept.release_id) as is_latest_release,
        list_filter([
            {%- for column in published_columns %}
            case when listed.{{ column }} is distinct from kept.{{ column }} then '{{ column }}' end{% if not loop.last %},{% endif %}
            {%- endfor %}
        ], field -> field is not null) as differing_fields
    from lineage as listed
    inner join kept on listed.event_key = kept.event_key
    where listed.event_rank > 1
),

holds as (
    select
        event_key,
        release_id,
        list_sort(list_distinct(flatten(array_agg(differing_fields)))) as differing_fields,
        case when bool_or(is_latest_release) then 'repeated_in_release' else 'differs_from_latest_release' end as hold_reason,
        list_sort(list_distinct(array_agg(member_sha256))) as member_sha256s
    from compared
    where is_latest_release or len(differing_fields) > 0
    group by
        event_key,
        release_id
)

select
    event_key,
    release_id,
    differing_fields,
    hold_reason,
    member_sha256s,
    md5(concat_ws(
        '|',
        'cms_change_of_ownership',
        hold_reason,
        coalesce(event_key, '<null>'),
        coalesce(release_id, '<null>'),
        '<null>',
        '<null>'
    )) as hold_key
from holds
