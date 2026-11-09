-- Fails for each spine row whose hospital-window POS fields differ from the arithmetic of the own POS snapshot it reads, and for each row whose snapshot
-- does not end before the window start (design 2.5 and 2.11). The expected values come from fct_pos_snapshot and literal rules, not from the spine model:
-- ORs per 100 beds is 100 * operating rooms / beds and is null when the beds are null or not above 0 (an operating room count of 0 stays 0), the log of
-- beds is null when the beds are not above 0, and the ownership class maps the POS control type (01 to 03 nonprofit, 04 and 09 for profit, 05 to 08 and 10
-- government). RNs per bed is not built: whether an RN count of 0 is missing or a real 0 is an open owner decision.

with

expected as (
    select
        spine.spine_key,
        pos.period_end as pos_period_end,
        spine.window_start,
        spine.or_per_100_beds,
        spine.log_bed_count,
        spine.ownership_class,
        case when pos.bed_count > 0 then 100.0 * pos.operating_room_count / pos.bed_count end as expected_or_per_100_beds,
        case when pos.bed_count > 0 then ln(pos.bed_count) end as expected_log_bed_count,
        case
            when spine.control_type_code in ('01', '02', '03') then 'nonprofit'
            when spine.control_type_code in ('04', '09') then 'for_profit'
            when spine.control_type_code in ('05', '06', '07', '08', '10') then 'government'
        end as expected_ownership_class
    from {{ ref('int_hospital_spine') }} as spine
    left join {{ ref('fct_pos_snapshot') }} as pos
        on
            spine.ccn = pos.ccn
            and spine.pos_period_end = pos.period_end
)

select
    spine_key,
    case
        when pos_period_end >= window_start then 'snapshot not before the start'
        when (or_per_100_beds is null) <> (expected_or_per_100_beds is null) or abs(or_per_100_beds - expected_or_per_100_beds) > 0.000000001
            then 'wrong ORs per 100 beds'
        when (log_bed_count is null) <> (expected_log_bed_count is null) or abs(log_bed_count - expected_log_bed_count) > 0.000000001 then 'wrong log of beds'
        else 'wrong ownership class'
    end as failure
from expected
where
    pos_period_end >= window_start
    or (or_per_100_beds is null) <> (expected_or_per_100_beds is null)
    or abs(or_per_100_beds - expected_or_per_100_beds) > 0.000000001
    or (log_bed_count is null) <> (expected_log_bed_count is null)
    or abs(log_bed_count - expected_log_bed_count) > 0.000000001
    or ownership_class is distinct from expected_ownership_class
