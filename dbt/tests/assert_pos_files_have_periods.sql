-- Fails for a POS file without exactly one period row, a period shared by two files or a period row naming no stored POS
-- file: every hospital snapshot has its catalog coverage [280] [281].
with

files as (
    select member_sha256
    from {{ ref('stg_bronze__files') }}
    where bronze_table = 'cms_provider_of_services'
),

periods as (
    select
        member_sha256,
        period_end
    from {{ ref('pos_file_periods') }}
),

period_counts as (
    select
        files.member_sha256,
        count(periods.member_sha256) as period_rows
    from files
    left join periods on files.member_sha256 = periods.member_sha256
    group by files.member_sha256
    having count(periods.member_sha256) <> 1
),

shared as (
    select
        min(member_sha256) as member_sha256,
        count(*) as period_rows
    from periods
    group by period_end
    having count(*) > 1
),

stale as (
    select
        periods.member_sha256,
        0 as period_rows
    from periods
    left join files on periods.member_sha256 = files.member_sha256
    where files.member_sha256 is null
)

select * from period_counts
union all
select * from shared
union all
select * from stale
