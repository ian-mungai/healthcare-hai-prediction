-- One row per stored file (member_sha256), plus the unknown member. object_key is the smallest object key among the file's stored copies and
-- copy_count counts them. release_date is the publication release read as a date and is null when the text is not a date. The period comes from
-- the file-period seeds where one covers the file, else it is null [824] [750].
with

copies as (
    select
        member_sha256,
        object_key,
        publication_release
    from {{ ref('stg_bronze__file_copies') }}
    where member_sha256 is not null
),

files as (
    select
        member_sha256,
        min(object_key) as object_key,
        count(*) as copy_count
    from copies
    group by member_sha256
),

releases as (
    select distinct
        files.member_sha256,
        files.object_key,
        files.copy_count,
        copies.publication_release
    from files
    inner join copies
        on
            files.member_sha256 = copies.member_sha256
            and files.object_key = copies.object_key
),

periods as (
    select
        member_sha256,
        period_start,
        period_end
    from {{ ref('geography_file_periods') }}
    union all
    select
        member_sha256,
        period_start,
        period_end
    from {{ ref('pos_file_periods') }}
    union all
    select
        member_sha256,
        period_start,
        period_end
    from {{ ref('ownership_release_periods') }}
),

final as (
    select
        md5(releases.member_sha256) as release_sk,
        releases.member_sha256,
        releases.object_key,
        releases.copy_count,
        try_cast(releases.publication_release as date) as release_date,
        periods.period_start,
        periods.period_end
    from releases
    left join periods
        on releases.member_sha256 = periods.member_sha256
    union all
    select
        md5('<unknown>') as release_sk,
        '<unknown>' as member_sha256,
        cast(null as varchar) as object_key,
        cast(null as bigint) as copy_count,
        cast(null as date) as release_date,
        cast(null as date) as period_start,
        cast(null as date) as period_end
)

select * from final
