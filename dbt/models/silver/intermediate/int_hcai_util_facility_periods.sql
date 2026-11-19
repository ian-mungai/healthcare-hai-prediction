-- One row per California HCAI utilization report of a facility for an operating period, from the page files (response_status responded) and the non-
-- response files (nonresponse). A facility that changed hands or closed has a part-year report beside its full-year one, so the period dates are part of
-- the grain. The period dates decode M/D/YYYY, YYYY-MM-DD and the Excel serial numbers of the workbook's date cells, and the published text stays in
-- period_begin_published and period_end_published. The numbers are the licensed beds, bed days, discharges, intra-transfers, census days, average
-- lengths of stay, patient counts, emergency visits, operating rooms, births and cardiac procedures of hcai_util_numeric_columns(), with the longitude
-- and latitude: typed once thousands separators are removed. A blank is null and a missing token (spreadsheet error text such as #DIV/0!, N/A) is null
-- with its text in nulled_values_published, a JSON object from column to text ('{}' when none). A row with any other number text, a facility that is no
-- number, a period that is no date or runs backwards, or a county name that does not identify exactly one county of California is held in
-- int_state_source_holds without its content, never typed. A non-response row with a facility number and every period cell empty (the real non-response
-- files list facilities and no period) is held as nonresponse_without_period; no period is invented for it. The person, phone and address cells never
-- leave bronze. The county code comes from the county
-- name within California (county_source name_match, from the state_county_names seed). The CCN comes only from the reviewed matches seed. The newest
-- release of a key is chosen among the rows with a usable key before the held rows are left out, so a newest row that is held leaves no older row marked
-- latest: the key has no latest row and the hold says why.
{{ config(materialized='table') }}

with

files as (
    select
        member_sha256,
        latest_publication_date,
        list_max(download_run_ids) as latest_download_run
    from {{ ref('stg_bronze__files') }}
    where bronze_table in ('hcai_util_pages_csv', 'hcai_util_nonresponse_csv')
),

county_keys as (
    {{ state_county_keys('CA') }}
),

matches as (
    select
        facility_key,
        ccn
    from {{ ref('state_facility_ccn_matches') }}
    where source_id = 'HCAI_UTIL'
),

sources as (
    {{ hcai_util_sources() }}
),

reasoned as (
    select
        sources.*,
        county_keys.county_fips,
        {{ hcai_util_hold_reason('sources', 'county_keys') }} as hold_reason
    from sources
    left join county_keys on {{ state_county_key('sources.county') }} = county_keys.county_key
),

typed as (
    select
        reasoned.bronze_table,
        reasoned.response_status,
        reasoned._member_sha256 as member_sha256,
        reasoned._row_number as source_row_number,
        reasoned.fac_no,
        reasoned.fac_name,
        reasoned.fac_city,
        case when regexp_full_match(trim(reasoned.fac_zip), '[0-9]{5}(-?[0-9]{4})?') then left(trim(reasoned.fac_zip), 5) end as zip_code,
        reasoned.county as county_name_published,
        reasoned.license_no,
        reasoned.license_status,
        reasoned.facility_level,
        reasoned.trauma_ctr,
        reasoned.teach_hosp,
        reasoned.teach_rural,
        coalesce(reasoned.census_key, reasoned.cens_tract) as census_tract_published,
        reasoned.lic_cat,
        reasoned.licee_toc,
        reasoned.prin_service_type,
        reasoned.emsa_trauma_designation,
        reasoned.emsa_trauma_designation_pediatric,
        reasoned.lic_ed_lev_begin,
        reasoned.lic_ed_lev_end,
        reasoned.fac_operated_this_yr,
        reasoned.fac_op_per_begin_dt as period_begin_published,
        reasoned.fac_op_per_end_dt as period_end_published,
        {{ state_date('reasoned.fac_op_per_begin_dt', true) }} as period_begin_date,
        {{ state_date('reasoned.fac_op_per_end_dt', true) }} as period_end_date,
        {%- for column in hcai_util_coordinate_columns() + hcai_util_numeric_columns() %}
        {{ state_number('reasoned.' ~ column) }} as {{ column }},
        {%- endfor %}
        to_json(map_from_entries(list_filter([
            {%- for column in hcai_util_coordinate_columns() + hcai_util_numeric_columns() %}
            {{ state_nulled_entry(column, 'reasoned.' ~ column) }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        ], entry -> entry.published is not null)))::varchar as nulled_values_published,
        reasoned.county_fips,
        files.latest_publication_date as publication_date,
        files.latest_download_run,
        reasoned.hold_reason
    from reasoned
    left join files on reasoned._member_sha256 = files.member_sha256
    where reasoned.hold_reason is null or reasoned.hold_reason in ('value_not_a_number', 'county_not_matched', 'period_reversed')
),

joined as (
    select
        typed.*,
        matches.ccn,
        'name_match' as county_source,
        case when matches.ccn is null then null else 'reviewed_match' end as ccn_source
    from typed
    left join matches on typed.fac_no = matches.facility_key
),

ranked as (
    select
        *,
        dense_rank() over (
            partition by fac_no, period_begin_date, period_end_date, response_status
            order by publication_date desc nulls last, latest_download_run desc nulls last, member_sha256 desc
        ) = 1 as is_latest_release
    from joined
)

select
    * exclude (latest_download_run, hold_reason),
    member_sha256 || ':' || source_row_number as util_row_key,
    year(period_end_date) as report_year
from ranked
where hold_reason is null
