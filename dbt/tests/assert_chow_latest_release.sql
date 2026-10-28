-- Fails for each change-of-ownership event whose kept row in int_change_of_ownership_rows is not the expected one, or
-- whose releases_listed is wrong. The expected row is derived step by step from
-- the staging rows and the release periods alone, with no macro or model that the checked model uses: the latest period end, then the
-- highest release ID among the rows of that period end, then the lowest source row (and checksum) among the rows of that
-- release. Duplicate kept rows are left to the unique test on event_key.
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
    -- mode 798). The event is the buyer enrollment, the seller enrollment, the effective date (M/D/YYYY, else blank) and the
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

latest_period as (
    select
        event_key,
        max(period_end) as period_end,
        count(distinct release_id) as releases
    from release_rows
    group by event_key
),

latest_release as (
    select
        release_rows.event_key,
        max(release_rows.release_id) as release_id
    from release_rows
    inner join latest_period
        on
            release_rows.event_key = latest_period.event_key
            and release_rows.period_end is not distinct from latest_period.period_end
    group by release_rows.event_key
),

expected as (
    select
        release_rows.event_key,
        latest_period.releases,
        min({ 'source_row_number': release_rows.source_row_number, 'member_sha256': release_rows.member_sha256 }) as first_row
    from release_rows
    inner join latest_period
        on
            release_rows.event_key = latest_period.event_key
            and release_rows.period_end is not distinct from latest_period.period_end
    inner join latest_release
        on
            release_rows.event_key = latest_release.event_key
            and release_rows.release_id is not distinct from latest_release.release_id
    group by
        release_rows.event_key,
        latest_period.releases
),

kept as (
    select
        event_key,
        member_sha256,
        source_row_number,
        releases_listed
    from {{ ref('int_change_of_ownership_rows') }}
),

missing_or_wrong as (
    select
        expected.event_key,
        'expected row not kept, or wrong releases_listed' as problem
    from expected
    left join kept
        on
            expected.event_key = kept.event_key
            and struct_extract(expected.first_row, 'member_sha256') = kept.member_sha256
            and struct_extract(expected.first_row, 'source_row_number') = kept.source_row_number
    where kept.event_key is null or kept.releases_listed is distinct from expected.releases
),

unknown_events as (
    select
        kept.event_key,
        'kept event not in any release' as problem
    from kept
    left join expected on kept.event_key = expected.event_key
    where expected.event_key is null
)

select * from missing_or_wrong
union all
select * from unknown_events
