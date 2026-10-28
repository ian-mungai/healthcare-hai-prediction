-- Fails for each POS hospital snapshot whose county_fips_published, county_fips, county_source or county_fill_reason
-- differs from what the published POS fields and the HUD crosswalk in force require. The expected values are derived here from the POS
-- source row (state, ZIP and FIPS parts), the file's period and the HUD quarters, never from the model's own columns: a
-- published county whose state part is the snapshot's state wins; otherwise one residential county of the ZIP under the
-- snapshot's state in the quarter containing the period end fills it (Connecticut: 090xx only); otherwise the reason.
with

source_rows as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        upper(nullif(trim(state_cd), '')) as state_code,
        case when regexp_full_match(trim(zip_cd), '[0-9]{5}(-?[0-9]{4})?') then left(trim(zip_cd), 5) end as zip_code,
        case
            when regexp_full_match(trim(fips_state_cd), '[0-9]{1,2}') and regexp_full_match(trim(fips_cnty_cd), '[0-9]{1,3}')
                then lpad(trim(fips_state_cd), 2, '0') || lpad(trim(fips_cnty_cd), 3, '0')
        end as published_county
    from {{ ref('stg_cms_provider_of_services') }}
    where regexp_full_match(trim(prvdr_ctgry_cd), '[0-9]{1,2}') and lpad(trim(prvdr_ctgry_cd), 2, '0') = '01'
),

periods as (
    select
        member_sha256,
        period_end
    from {{ ref('pos_file_periods') }}
),

state_fips (state_code, state_fips) as (
    values
    ('AL', '01'), ('AK', '02'), ('AZ', '04'), ('AR', '05'), ('CA', '06'), ('CO', '08'), ('CT', '09'), ('DE', '10'),
    ('DC', '11'), ('FL', '12'), ('GA', '13'), ('HI', '15'), ('ID', '16'), ('IL', '17'), ('IN', '18'), ('IA', '19'),
    ('KS', '20'), ('KY', '21'), ('LA', '22'), ('ME', '23'), ('MD', '24'), ('MA', '25'), ('MI', '26'), ('MN', '27'),
    ('MS', '28'), ('MO', '29'), ('MT', '30'), ('NE', '31'), ('NV', '32'), ('NH', '33'), ('NJ', '34'), ('NM', '35'),
    ('NY', '36'), ('NC', '37'), ('ND', '38'), ('OH', '39'), ('OK', '40'), ('OR', '41'), ('PA', '42'), ('RI', '44'),
    ('SC', '45'), ('SD', '46'), ('TN', '47'), ('TX', '48'), ('UT', '49'), ('VT', '50'), ('VA', '51'), ('WA', '53'),
    ('WV', '54'), ('WI', '55'), ('WY', '56'), ('AS', '60'), ('FM', '64'), ('GU', '66'), ('MH', '68'), ('MP', '69'),
    ('PW', '70'), ('PR', '72'), ('UM', '74'), ('VI', '78')
),

hud as (
    select
        zip_code,
        county_fips,
        usps_state,
        quarter_start_date,
        quarter_end_date,
        res_ratio
    from {{ ref('int_hud_zip_county_quarters') }}
),

quarters as (
    select distinct
        quarter_start_date,
        quarter_end_date
    from hud
),

snapshots as (
    select
        source_rows.*,
        periods.period_end,
        coalesce(left(source_rows.published_county, 2) = state_fips.state_fips, false) as is_published_valid
    from source_rows
    left join periods on source_rows.member_sha256 = periods.member_sha256
    left join state_fips on source_rows.state_code = state_fips.state_code
),

in_force as (
    -- Whether any HUD quarter contains the period end, and the ZIP's counties in it: residential, under the state, and
    -- eligible to fill (090xx only in Connecticut).
    select
        snapshots.member_sha256,
        snapshots.source_row_number,
        any_value(quarters.quarter_start_date) is not null as has_quarter,
        count(distinct hud.county_fips) filter (where hud.res_ratio > 0) as residential_counties,
        count(distinct hud.county_fips) filter (where hud.res_ratio > 0 and hud.usps_state = snapshots.state_code) as state_counties,
        count(distinct hud.county_fips) filter (
            where hud.res_ratio > 0 and hud.usps_state = snapshots.state_code and (snapshots.state_code <> 'CT' or hud.county_fips like '090%')
        ) as eligible_counties,
        min(hud.county_fips) filter (
            where hud.res_ratio > 0 and hud.usps_state = snapshots.state_code and (snapshots.state_code <> 'CT' or hud.county_fips like '090%')
        ) as eligible_county
    from snapshots
    left join quarters
        on snapshots.period_end between quarters.quarter_start_date and quarters.quarter_end_date
    left join hud
        on
            snapshots.zip_code = hud.zip_code
            and snapshots.period_end between hud.quarter_start_date and hud.quarter_end_date
    group by
        snapshots.member_sha256,
        snapshots.source_row_number
),

expected as (
    select
        snapshots.member_sha256,
        snapshots.source_row_number,
        snapshots.published_county as expected_published,
        case
            when snapshots.is_published_valid then snapshots.published_county
            when snapshots.zip_code is not null and in_force.has_quarter and in_force.eligible_counties = 1 then in_force.eligible_county
        end as expected_county,
        case
            when snapshots.is_published_valid then 'pos'
            when snapshots.zip_code is not null and in_force.has_quarter and in_force.eligible_counties = 1 then 'hud_zip_crosswalk'
        end as expected_source,
        case
            when snapshots.is_published_valid then null
            when snapshots.zip_code is null then 'no_zip'
            when not in_force.has_quarter then 'no_quarter_in_force'
            when in_force.residential_counties = 0 then 'no_residential_county'
            when in_force.state_counties = 0 then 'zip_state_mismatch'
            when in_force.eligible_counties = 0 then 'connecticut_planning_region'
            when in_force.eligible_counties > 1 then 'ambiguous_zip'
        end as expected_reason
    from snapshots
    inner join in_force
        on
            snapshots.member_sha256 = in_force.member_sha256
            and snapshots.source_row_number = in_force.source_row_number
),

actual as (
    select
        member_sha256,
        source_row_number,
        snapshot_key,
        county_fips_published,
        county_fips,
        county_source,
        county_fill_reason
    from {{ ref('int_pos_hospital_snapshots') }}
)

select
    actual.snapshot_key,
    expected.expected_published,
    actual.county_fips_published,
    expected.expected_county,
    actual.county_fips,
    expected.expected_source,
    actual.county_source,
    expected.expected_reason,
    actual.county_fill_reason,
    coalesce(expected.member_sha256, actual.member_sha256) as member_sha256,
    coalesce(expected.source_row_number, actual.source_row_number) as source_row_number
from expected
full outer join actual
    on
        expected.member_sha256 = actual.member_sha256
        and expected.source_row_number = actual.source_row_number
where
    expected.member_sha256 is null
    or actual.member_sha256 is null
    or expected.expected_published is distinct from actual.county_fips_published
    or expected.expected_county is distinct from actual.county_fips
    or expected.expected_source is distinct from actual.county_source
    or expected.expected_reason is distinct from actual.county_fill_reason
