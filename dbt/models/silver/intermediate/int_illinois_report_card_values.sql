-- One row per Illinois Hospital Report Card value: entity (the report card's hospital number), measure and period, from each stored release. The value,
-- numerator and denominator keep their published text and type as numbers (value_number, numerator, denominator) once thousands separators are
-- removed; a missing token is null and a value that is neither fails assert_state_values_cast. The county code comes from the county name within
-- Illinois (county_source name_match, from the state_county_names seed), only when exactly one county has that name. The CCN comes only from the
-- reviewed matches seed. The measure texts (description, rationale, evidence) stay in bronze. is_latest_release marks the row of the newest release
-- of each key (publication date, then download run). Rows whose entity, measure or period cannot be keyed are held in int_state_source_holds.
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
    select
        {{ state_county_key('county_name') }} as county_key,
        min(county_fips) as county_fips
    from {{ ref('state_county_names') }}
    where state = 'IL'
    group by {{ state_county_key('county_name') }}
    having count(distinct county_fips) = 1
),

matches as (
    select
        facility_key,
        ccn
    from {{ ref('state_facility_ccn_matches') }}
    where source_id = 'IL'
),

typed as (
    select
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        trim(stg.entity_id) as entity_id,
        nullif(trim(stg.name), '') as facility_name,
        nullif(trim(stg.city), '') as city,
        nullif(trim(stg.county_name), '') as county_name_published,
        nullif(trim(stg.aha_service_id), '') as aha_service_id,
        trim(stg.measure_id) as measure_id,
        nullif(trim(stg.name_friendly), '') as measure_name,
        nullif(trim(stg.measure_type), '') as measure_type,
        nullif(trim(stg.default_unit), '') as default_unit,
        {{ state_date('stg.date_start') }} as period_start,
        {{ state_date('stg.date_end') }} as period_end,
        nullif(trim(stg.value), '') as value_published,
        {{ state_number('stg.value') }} as value_number,
        nullif(trim(stg.numerator), '') as numerator_published,
        {{ state_number('stg.numerator') }} as numerator,
        nullif(trim(stg.denominator), '') as denominator_published,
        {{ state_number('stg.denominator') }} as denominator,
        files.latest_publication_date as publication_date,
        files.latest_download_run
    from {{ ref('stg_illinois_report_card_csv') }} as stg
    left join files on stg._member_sha256 = files.member_sha256
    where {{ illinois_hold_reason('stg') }} is null
),

joined as (
    select
        typed.*,
        county_keys.county_fips,
        matches.ccn,
        case when county_keys.county_fips is null then 'not_matched' else 'name_match' end as county_source,
        case when matches.ccn is null then null else 'reviewed_match' end as ccn_source
    from typed
    left join county_keys on {{ state_county_key('typed.county_name_published') }} = county_keys.county_key
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
