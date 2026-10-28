-- Fails for each change-of-ownership hold that int_change_of_ownership_holds is missing, has without cause or writes
-- differently from the hold derived here. Each release row of an event
-- other than the kept row is compared with the kept row field by field on the 23 published columns, through both rows'
-- lineage to the staging rows (one long row per field, nulls kept). A row in the kept row's release is held as
-- repeated_in_release; a row of another release is held as differs_from_latest_release when a published field differs.
-- Per event and release the hold must have exactly the expected reason, sorted differing fields, sorted checksums of the
-- held rows and the A5 key.
{%- set published_columns = [
    'enrollment_id_buyer', 'enrollment_state_buyer', 'provider_type_code_buyer', 'provider_type_text_buyer', 'npi_buyer',
    'multiple_npi_flag_buyer', 'ccn_buyer', 'associate_id_buyer', 'organization_name_buyer', 'doing_business_as_name_buyer',
    'chow_type_code', 'chow_type_text', 'effective_date', 'enrollment_id_seller', 'enrollment_state_seller',
    'provider_type_code_seller', 'provider_type_text_seller', 'npi_seller', 'multiple_npi_flag_seller', 'ccn_seller',
    'associate_id_seller', 'organization_name_seller', 'doing_business_as_name_seller'
] %}
with

periods as (
    select
        member_sha256,
        release_id,
        period_end
    from {{ ref('ownership_release_periods') }}
    where bronze_table = 'cms_change_of_ownership'
),

release_rows as (
    -- From the staging rows and the release periods alone, with no macro or model that the checked models use (failure
    -- mode 799). The event is the buyer enrollment, the seller enrollment, the effective date (M/D/YYYY, else blank) and the
    -- upper-case type.
    select
        periods.release_id,
        periods.period_end,
        staged._member_sha256 as member_sha256,
        staged._row_number as source_row_number,
        concat_ws(
            '|',
            trim(coalesce(staged.enrollment_id_buyer, '')),
            trim(coalesce(staged.enrollment_id_seller, '')),
            case
                when regexp_full_match(trim(staged.effective_date), '[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')
                    then coalesce(strftime(try_strptime(trim(staged.effective_date), '%m/%d/%Y'), '%Y-%m-%d'), '')
                else ''
            end,
            upper(trim(coalesce(staged.chow_type_code, '')))
        ) as event_key
    from {{ ref('stg_cms_change_of_ownership') }} as staged
    left join periods on staged._member_sha256 = periods.member_sha256
),

kept as (
    -- The kept row of each event: the latest period end, then the highest release ID, then the lowest source row and checksum.
    select
        event_key,
        release_id,
        member_sha256,
        source_row_number
    from (
        select
            *,
            row_number() over (
                partition by event_key
                order by period_end desc nulls last, release_id desc nulls last, source_row_number asc, member_sha256 asc
            ) as event_rank
        from release_rows
    ) as ranked
    where event_rank = 1
),

published_fields as (
    -- One row per staging row and published column, with nulls kept.
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        unnest({{ published_columns }}) as field,
        unnest([
            {%- for column in published_columns %}
            {{ column }}::varchar{% if not loop.last %},{% endif %}
            {%- endfor %}
        ]) as published_value
    from {{ ref('stg_cms_change_of_ownership') }}
),

others as (
    select
        release_rows.event_key,
        release_rows.release_id,
        release_rows.member_sha256,
        release_rows.source_row_number,
        kept.member_sha256 as kept_member_sha256,
        kept.source_row_number as kept_source_row_number,
        (release_rows.release_id is not distinct from kept.release_id) as is_kept_release
    from release_rows
    inner join kept on release_rows.event_key = kept.event_key
    where not (release_rows.member_sha256 = kept.member_sha256 and release_rows.source_row_number = kept.source_row_number)
),

field_differences as (
    select
        others.member_sha256,
        others.source_row_number,
        listed.field
    from others
    inner join published_fields as listed
        on
            others.member_sha256 = listed.member_sha256
            and others.source_row_number = listed.source_row_number
    inner join published_fields as kept_fields
        on
            others.kept_member_sha256 = kept_fields.member_sha256
            and others.kept_source_row_number = kept_fields.source_row_number
            and listed.field = kept_fields.field
    where listed.published_value is distinct from kept_fields.published_value
),

row_differences as (
    select
        others.event_key,
        others.release_id,
        others.member_sha256,
        others.is_kept_release,
        coalesce(list_sort(array_agg(field_differences.field) filter (where field_differences.field is not null)), []::varchar[])
            as row_fields
    from others
    left join field_differences
        on
            others.member_sha256 = field_differences.member_sha256
            and others.source_row_number = field_differences.source_row_number
    group by
        others.event_key,
        others.release_id,
        others.member_sha256,
        others.source_row_number,
        others.is_kept_release
),

expected_reasons as (
    select
        event_key,
        release_id,
        case when bool_or(is_kept_release) then 'repeated_in_release' else 'differs_from_latest_release' end as hold_reason,
        list_sort(list_distinct(flatten(array_agg(row_fields)))) as differing_fields,
        list_sort(list_distinct(array_agg(member_sha256))) as member_sha256s
    from row_differences
    where is_kept_release or len(row_fields) > 0
    group by
        event_key,
        release_id
),

expected as (
    select
        *,
        md5(
            'cms_change_of_ownership|' || hold_reason || '|' || coalesce(event_key, '<null>') || '|' || coalesce(release_id, '<null>')
            || '|<null>|<null>'
        ) as hold_key
    from expected_reasons
),

holds as (
    select
        event_key,
        release_id,
        hold_reason,
        differing_fields,
        member_sha256s,
        hold_key
    from {{ ref('int_change_of_ownership_holds') }}
),

hold_problems as (
    select
        'hold differs' as problem,
        coalesce(expected.event_key, holds.event_key) as event_key,
        coalesce(expected.release_id, holds.release_id) as release_id,
        concat_ws(
            ' | ',
            'expected ' || coalesce(expected.hold_reason, '<none>') || ' ' || coalesce(expected.differing_fields::varchar, '<none>'),
            'held ' || coalesce(holds.hold_reason, '<none>') || ' ' || coalesce(holds.differing_fields::varchar, '<none>'),
            'expected members ' || coalesce(expected.member_sha256s::varchar, '<none>'),
            'held members ' || coalesce(holds.member_sha256s::varchar, '<none>'),
            'expected key ' || coalesce(expected.hold_key, '<none>') || ' held key ' || coalesce(holds.hold_key, '<none>')
        ) as detail
    from expected
    full outer join holds
        on
            expected.event_key = holds.event_key
            and expected.release_id is not distinct from holds.release_id
    where
        expected.event_key is null
        or holds.event_key is null
        or expected.hold_reason is distinct from holds.hold_reason
        or expected.differing_fields is distinct from holds.differing_fields
        or expected.member_sha256s is distinct from holds.member_sha256s
        or expected.hold_key is distinct from holds.hold_key
),

kept_problems as (
    -- The holds are measured against the kept row derived above; the rows model must keep that same row, or the holds and the
    -- rows model describe different events.
    select
        'rows model keeps another row' as problem,
        coalesce(kept.event_key, rows_model.event_key) as event_key,
        coalesce(kept.release_id, rows_model.release_id) as release_id,
        'expected ' || coalesce(kept.member_sha256, '<none>') || ':' || coalesce(kept.source_row_number::varchar, '<none>')
        || ' kept ' || coalesce(rows_model.member_sha256, '<none>') || ':' || coalesce(rows_model.source_row_number::varchar, '<none>')
            as detail
    from kept
    full outer join {{ ref('int_change_of_ownership_rows') }} as rows_model
        on kept.event_key = rows_model.event_key
    where
        kept.event_key is null
        or rows_model.event_key is null
        or kept.member_sha256 is distinct from rows_model.member_sha256
        or kept.source_row_number is distinct from rows_model.source_row_number
)

select * from hold_problems
union all
select * from kept_problems
