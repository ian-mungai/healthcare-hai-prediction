-- One row per Illinois Hospital Report Card value: entity (the report card's hospital number), measure and period, from each stored release. The value,
-- numerator and denominator keep their published text and type as numbers (value_number, numerator, denominator) once thousands separators are removed;
-- a missing token is null. A row with any other number text, an entity or measure that is no number, a period that is no date or runs backwards, or a
-- county name that does not identify exactly one county of Illinois is held in int_state_source_holds without its content, never typed. The county code
-- comes from the county name within Illinois (county_source name_match, from the state_county_names seed). The CCN comes only from the reviewed matches
-- seed. The measure texts (description, rationale, evidence) stay in bronze. is_latest_release marks the row of the newest release of each key
-- (publication date, then download run).
{{ config(materialized='table') }}

with

files as (
    select
        member_sha256,
        latest_publication_date,
        list_max(download_run_ids) as latest_download_run
    from {{ ref('stg_bronze__files') }}
    where bronze_table = 'illinois_report_card_csv'
),

county_keys as (
    {{ state_county_keys('IL') }}
),

matches as (
    select
        facility_key,
        ccn
    from {{ ref('state_facility_ccn_matches') }}
    where source_id = 'IL'
),

reasoned as (
    select
        stg.*,
        county_keys.county_fips,
        {{ illinois_hold_reason('stg', 'county_keys') }} as hold_reason
    from {{ ref('stg_illinois_report_card_csv') }} as stg
    left join county_keys on {{ state_county_key('stg.county_name') }} = county_keys.county_key
),

typed as (
    select
        reasoned._member_sha256 as member_sha256,
        reasoned._row_number as source_row_number,
        reasoned.entity_id,
        reasoned.name as facility_name,
        reasoned.city,
        reasoned.county_name as county_name_published,
        reasoned.aha_service_id,
        reasoned.measure_id,
        reasoned.name_friendly as measure_name,
        reasoned.measure_type,
        reasoned.default_unit,
        {{ state_date('reasoned.date_start') }} as period_start,
        {{ state_date('reasoned.date_end') }} as period_end,
        nullif(trim(reasoned.value), '') as value_published,
        {{ state_number('reasoned.value') }} as value_number,
        nullif(trim(reasoned.numerator), '') as numerator_published,
        {{ state_number('reasoned.numerator') }} as numerator,
        nullif(trim(reasoned.denominator), '') as denominator_published,
        {{ state_number('reasoned.denominator') }} as denominator,
        reasoned.county_fips,
        files.latest_publication_date as publication_date,
        files.latest_download_run
    from reasoned
    left join files on reasoned._member_sha256 = files.member_sha256
    where reasoned.hold_reason is null
),

joined as (
    select
        typed.*,
        matches.ccn,
        'name_match' as county_source,
        case when matches.ccn is null then null else 'reviewed_match' end as ccn_source
    from typed
    left join matches on typed.entity_id = matches.facility_key
)

select
    * exclude (latest_download_run),
    member_sha256 || ':' || source_row_number as il_row_key,
    dense_rank() over (
        partition by entity_id, measure_id, period_start, period_end
        order by publication_date desc nulls last, latest_download_run desc nulls last, member_sha256 desc
    ) = 1 as is_latest_release
from joined
