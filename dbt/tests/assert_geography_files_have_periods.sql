-- Fails for a loaded C1 geography file without exactly one period row, a HUD or service-area file without its period
-- dates, or a period row naming no stored file: every file is dated by its receipt quarter, its job plan's catalog
-- coverage or its reviewed vintage [403] [410] [416]. The HUD workbooks are twins of the CSVs and need no period.
with

files as (
    select
        bronze_table,
        member_sha256
    from {{ ref('stg_bronze__files') }}
    where source_family = 'geography' and bronze_table <> 'hud_zip_county_sheet_rows'
),

periods as (
    select
        bronze_table,
        member_sha256,
        vintage,
        period_start
    from {{ ref('geography_file_periods') }}
),

period_counts as (
    select
        files.bronze_table,
        files.member_sha256,
        'period rows: ' || count(periods.member_sha256) as problem
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

undated as (
    select
        bronze_table,
        member_sha256,
        'no period dates' as problem
    from periods
    where
        vintage is null
        or (bronze_table in ('hud_zip_county', 'cms_hsa_csv') and period_start is null)
),

stale as (
    select
        periods.bronze_table,
        periods.member_sha256,
        'no stored file' as problem
    from periods
    left join files
        on
            periods.bronze_table = files.bronze_table
            and periods.member_sha256 = files.member_sha256
    where files.member_sha256 is null
)

select * from period_counts
union all
select * from undated
union all
select * from stale
