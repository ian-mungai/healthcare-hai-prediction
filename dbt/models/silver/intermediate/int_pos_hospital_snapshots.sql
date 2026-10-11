-- One row per hospital (CCN) and Provider of Services snapshot: the snapshot period from the catalog, padded codes,
-- 5-character county FIPS, typed counts, switches and dates, and the flags the model population uses (failure modes 280
-- to 292). Every hospital row stays, terminated or outside the 50 states and DC; later models filter.
-- County fill (decision 1, silver section 7, owner decision A of Oct 11 2026; failure modes 796 and 797): a valid published
-- county wins (county_source pos). A missing county, or one whose state part is not a US state, DC or territory or not the
-- snapshot's state, takes the county of the snapshot's ZIP when the HUD quarter in force on the period end (the quarter
-- whose start and end contain it) lists the ZIP with a residential ratio above 0 under the snapshot's state, in exactly one
-- county (county_source hud_zip_crosswalk; Connecticut only from 090xx county codes). Otherwise the county stays null and
-- county_fill_reason says why. county_fips_published keeps the published county.
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        period_start,
        period_end
    from {{ ref('pos_file_periods') }}
),

hospitals as (
    -- Provider category 01 is a hospital in every layout [282].
    select *
    from {{ ref('stg_cms_provider_of_services') }}
    where {{ pos_typed_value('prvdr_ctgry_cd', 'code', 2) }} = '01'
),

typed as (
    select
        _member_sha256 as member_sha256,
        _object_key as object_key,
        _row_number as source_row_number,
        nullif(trim(prvdr_num), '') as provider_number,
        nullif(trim(fac_name), '') as facility_name,
        upper(nullif(trim(state_cd), '')) as state_code,
        case when regexp_full_match(trim(zip_cd), '[0-9]{5}(-?[0-9]{4})?') then left(trim(zip_cd), 5) end as zip_code,
        upper(nullif(trim(cbsa_urbn_rrl_ind), '')) as cbsa_urban_rural_code,
        {%- for column, name, kind, width in pos_typed_columns() %}
        {{ pos_typed_value(column, kind, width) }} as {{ name }},
        {%- endfor %}
        -- A CCN is 6 characters of digits and capital letters, never cast to a number [289].
        case when regexp_full_match(trim(prvdr_num), '[0-9A-Z]{6}') then trim(prvdr_num) end as ccn
    from hospitals
),

state_fips (state_code, state_fips) as (
    -- The FIPS state code of each USPS code of the 50 states, DC and the territories [796].
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

snapshots as (
    select
        typed.*,
        periods.period_start,
        periods.period_end,
        coalesce(typed.ccn, typed.provider_number) || ':' || periods.period_end as snapshot_key,
        -- A county needs both padded parts; either missing leaves it null [285].
        typed.fips_state_code || typed.fips_county_code as county_fips_published,
        -- A published county is valid when its state part is the snapshot's state (a US state, DC or territory) [796].
        coalesce(left(typed.fips_state_code || typed.fips_county_code, 2) = state_fips.state_fips, false) as has_valid_county
    from typed
    -- A file without a period keeps its rows with a null period, which the tests reject [281].
    left join periods on typed.member_sha256 = periods.member_sha256
    left join state_fips on typed.state_code = state_fips.state_code
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

to_fill as (
    -- Snapshots with a ZIP and no valid published county, and whether a HUD quarter is in force on their period end (a
    -- snapshot without a period has none).
    select
        snapshots.member_sha256,
        snapshots.source_row_number,
        snapshots.state_code,
        snapshots.zip_code,
        snapshots.period_end,
        exists(
            select 1
            from quarters
            where snapshots.period_end between quarters.quarter_start_date and quarters.quarter_end_date
        ) as has_quarter
    from snapshots
    where not snapshots.has_valid_county and snapshots.zip_code is not null
),

zip_counties as (
    -- The ZIP's residential counties in the quarter in force, split by state match and Connecticut code system [796].
    select
        to_fill.member_sha256,
        to_fill.source_row_number,
        count(hud.county_fips) as residential_rows,
        count(distinct hud.county_fips) filter (
            where hud.usps_state = to_fill.state_code and (to_fill.state_code <> 'CT' or hud.county_fips like '090%')
        ) as counties,
        count(*) filter (where hud.usps_state = to_fill.state_code) as state_rows,
        min(hud.county_fips) filter (
            where hud.usps_state = to_fill.state_code and (to_fill.state_code <> 'CT' or hud.county_fips like '090%')
        ) as county_fips
    from to_fill
    inner join hud
        on
            to_fill.zip_code = hud.zip_code
            and to_fill.period_end between hud.quarter_start_date and hud.quarter_end_date
            and hud.res_ratio > 0
    group by
        to_fill.member_sha256,
        to_fill.source_row_number
),

fills as (
    select
        to_fill.member_sha256,
        to_fill.source_row_number,
        case when zip_counties.counties = 1 then zip_counties.county_fips end as county_fips,
        case
            when not to_fill.has_quarter then 'no_quarter_in_force'
            when coalesce(zip_counties.residential_rows, 0) = 0 then 'no_residential_county'
            when zip_counties.state_rows = 0 then 'zip_state_mismatch'
            when zip_counties.counties = 0 then 'connecticut_planning_region'
            when zip_counties.counties > 1 then 'ambiguous_zip'
        end as county_fill_reason
    from to_fill
    left join zip_counties
        on
            to_fill.member_sha256 = zip_counties.member_sha256
            and to_fill.source_row_number = zip_counties.source_row_number
),

filled as (
    select
        snapshots.*,
        fills.county_fips as filled_county_fips,
        fills.county_fill_reason as filled_reason
    from snapshots
    left join fills
        on
            snapshots.member_sha256 = fills.member_sha256
            and snapshots.source_row_number = fills.source_row_number
),

final as (
    select
        * exclude (has_valid_county, filled_county_fips, filled_reason),
        case when has_valid_county then county_fips_published else filled_county_fips end as county_fips,
        ssa_state_code || ssa_county_part as ssa_county_code,
        termination_code = '00' as is_active,
        coalesce(state_code in {{ us_states_and_dc() }}, false) as is_state_or_dc,
        coalesce(state_code = 'CT', false) as is_connecticut,
        coalesce(substr(ccn, 3, 2) = '13', false) as is_critical_access_by_ccn,
        coalesce(right(ccn, 1) = 'F', false) as is_veterans_affairs,
        case
            when has_valid_county then 'pos'
            when filled_county_fips is not null then 'hud_zip_crosswalk'
        end as county_source,
        case
            when has_valid_county then null
            when zip_code is null then 'no_zip'
            else filled_reason
        end as county_fill_reason
    from filled
)

select
    * exclude (county_fips_published, county_source, county_fill_reason),
    county_fips_published,
    county_source,
    county_fill_reason
from final
