-- Fails for a HUD ZIP code and quarter whose residential ratios sum to neither 0 (no residential addresses) nor 1, within
-- 0.01 for rounding. Held non-county rows count, since HUD's denominators include them [406].
with

ratios as (
    select
        _member_sha256 as member_sha256,
        zip,
        {{ strict_number('res_ratio') }} as res_ratio
    from {{ ref('stg_hud_zip_county') }}
),

sums as (
    select
        member_sha256,
        zip,
        sum(res_ratio) as residential_sum
    from ratios
    group by
        member_sha256,
        zip
)

select
    member_sha256,
    zip,
    residential_sum
from sums
where abs(residential_sum) > 0.01 and abs(residential_sum - 1) > 0.01
