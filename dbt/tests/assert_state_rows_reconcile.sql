-- Fails for each stored state-source file whose staged rows are not its typed rows plus its held rows, and for each non-empty file with no typed row: a
-- build that turns every row of a file into holds, or loses rows, must not pass.
with

staged as (
    select
        'hcai_util_pages_csv' as bronze_table,
        _member_sha256 as member_sha256,
        count(*) as staged_rows
    from {{ ref('stg_hcai_util_pages_csv') }}
    group by _member_sha256
    union all
    select
        'hcai_util_nonresponse_csv' as bronze_table,
        _member_sha256 as member_sha256,
        count(*) as staged_rows
    from {{ ref('stg_hcai_util_nonresponse_csv') }}
    group by _member_sha256
    union all
    select
        'illinois_report_card_csv' as bronze_table,
        _member_sha256 as member_sha256,
        count(*) as staged_rows
    from {{ ref('stg_illinois_report_card_csv') }}
    group by _member_sha256
),

typed as (
    select
        bronze_table,
        member_sha256,
        count(*) as typed_rows
    from {{ ref('int_hcai_util_facility_periods') }}
    group by bronze_table, member_sha256
    union all
    select
        'illinois_report_card_csv' as bronze_table,
        member_sha256,
        count(*) as typed_rows
    from {{ ref('int_illinois_report_card_values') }}
    group by member_sha256
),

held as (
    select
        bronze_table,
        member_sha256,
        count(*) as held_rows
    from {{ ref('int_state_source_holds') }}
    group by bronze_table, member_sha256
)

select
    staged.bronze_table,
    staged.member_sha256,
    staged.staged_rows,
    coalesce(typed.typed_rows, 0) as typed_rows,
    coalesce(held.held_rows, 0) as held_rows
from staged
left join typed on staged.bronze_table = typed.bronze_table and staged.member_sha256 = typed.member_sha256
left join held on staged.bronze_table = held.bronze_table and staged.member_sha256 = held.member_sha256
where
    staged.staged_rows <> coalesce(typed.typed_rows, 0) + coalesce(held.held_rows, 0)
    or coalesce(typed.typed_rows, 0) = 0
