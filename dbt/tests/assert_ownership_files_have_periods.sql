-- Fails for an owner, enrollment or change-of-ownership file without exactly one period row, a period two files of one
-- table share or a period row naming no stored file: every release is dated by its recorded catalog period [365].
with

files as (
    select
        bronze_table,
        member_sha256
    from {{ ref('stg_bronze__files') }}
    where bronze_table in ('cms_hospital_owners', 'cms_hospital_enrollments', 'cms_change_of_ownership')
),

periods as (
    select
        bronze_table,
        member_sha256,
        period_end
    from {{ ref('ownership_release_periods') }}
),

period_counts as (
    select
        files.bronze_table,
        files.member_sha256,
        count(periods.member_sha256) as period_rows
    from files
    left join periods
        on
            files.bronze_table = periods.bronze_table
            and files.member_sha256 = periods.member_sha256
    group by
        files.bronze_table,
        files.member_sha256
    having count(periods.member_sha256) <> 1
),

shared as (
    select
        bronze_table,
        min(member_sha256) as member_sha256,
        count(*) as period_rows
    from periods
    group by
        bronze_table,
        period_end
    having count(*) > 1
),

stale as (
    select
        periods.bronze_table,
        periods.member_sha256,
        0 as period_rows
    from periods
    left join files
        on
            periods.bronze_table = files.bronze_table
            and periods.member_sha256 = files.member_sha256
    where files.member_sha256 is null
)

select * from period_counts
union all
select * from shared
union all
select * from stale
