with

copies as (
    select
        member_sha256,
        object_key,
        publication_date
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
        copies.publication_date
    from files
    inner join copies
        on
            files.member_sha256 = copies.member_sha256
            and files.object_key = copies.object_key
),

release_lists as (
    select
        member_sha256,
        publication_dates,
        download_run_ids
    from {{ ref('stg_bronze__files') }}
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
        releases.publication_date as release_date,
        periods.period_start,
        periods.period_end,
        release_lists.publication_dates,
        release_lists.download_run_ids
    from releases
    inner join release_lists on releases.member_sha256 = release_lists.member_sha256
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
        cast(null as date) as period_end,
        cast(null as date[]) as publication_dates,
        cast(null as varchar[]) as download_run_ids
)

select * from final
