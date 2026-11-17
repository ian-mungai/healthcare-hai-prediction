-- One row per California HCAI utilization facility report: facility number (fac_no), operating period begin and end, and stored release, from the page files
-- (response_status responded) and the non-response files (nonresponse). A facility that changed hands or closed has a part-year report beside its
-- full-year one, so the period dates are part of the grain. The numbers are the licensed beds, bed days, discharges, intra-transfers, census days, average
-- lengths of stay, patient counts, emergency visits, operating rooms, births and cardiac procedures of hcai_util_numeric_columns(): typed once thousands
-- separators are removed. A blank is null; any other value that does not type (spreadsheet error text such as #DIV/0!, N/A) is null and its published
-- text is kept in nulled_values_published, a JSON object from column to text ('{}' when none). A text that is neither a number nor a reviewed missing token
-- fails assert_state_values_cast. The person, phone and address cells (hcai_util_withheld_columns()) are never read. The county code comes from the
-- county name within California (county_source name_match, from the state_county_names seed), only when exactly one county has that name. The CCN
-- comes only from the reviewed matches seed. The description row and rows without a period are held in int_state_source_holds.
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
    select
        {{ state_county_key('county_name') }} as county_key,
        min(county_fips) as county_fips
    from {{ ref('state_county_names') }}
    where state = 'CA'
    group by {{ state_county_key('county_name') }}
    having count(distinct county_fips) = 1
),

matches as (
    select
        facility_key,
        ccn
    from {{ ref('state_facility_ccn_matches') }}
    where source_id = 'HCAI_UTIL'
),

sources as (
    select
        'hcai_util_pages_csv' as bronze_table,
        'responded' as response_status,
        *
    from {{ ref('stg_hcai_util_pages_csv') }}
    union all by name
    select
        'hcai_util_nonresponse_csv' as bronze_table,
        'nonresponse' as response_status,
        *
    from {{ ref('stg_hcai_util_nonresponse_csv') }}
),

typed as (
    select
        sources.bronze_table,
        sources.response_status,
        sources._member_sha256 as member_sha256,
        sources._row_number as source_row_number,
        trim(sources.fac_no) as fac_no,
        nullif(trim(sources.fac_name), '') as fac_name,
        nullif(trim(sources.fac_city), '') as fac_city,
        case when regexp_full_match(trim(sources.fac_zip), '[0-9]{5}(-?[0-9]{4})?') then left(trim(sources.fac_zip), 5) end as zip_code,
        nullif(trim(sources.county), '') as county_name_published,
        nullif(trim(sources.license_no), '') as license_no,
        nullif(trim(sources.license_status), '') as license_status,
        nullif(trim(sources.facility_level), '') as facility_level,
        nullif(trim(sources.trauma_ctr), '') as trauma_ctr,
        nullif(trim(sources.teach_hosp), '') as teach_hosp,
        nullif(trim(sources.teach_rural), '') as teach_rural,
        {{ state_number('sources.longitude') }} as longitude,
        {{ state_number('sources.latitude') }} as latitude,
        nullif(trim(coalesce(sources.census_key, sources.cens_tract)), '') as census_tract_published,
        nullif(trim(sources.lic_cat), '') as lic_cat,
        nullif(trim(sources.licee_toc), '') as licee_toc,
        nullif(trim(sources.prin_service_type), '') as prin_service_type,
        nullif(trim(sources.emsa_trauma_designation), '') as emsa_trauma_designation,
        nullif(trim(sources.emsa_trauma_designation_pediatric), '') as emsa_trauma_designation_pediatric,
        nullif(trim(sources.lic_ed_lev_begin), '') as lic_ed_lev_begin,
        nullif(trim(sources.lic_ed_lev_end), '') as lic_ed_lev_end,
        nullif(trim(sources.fac_operated_this_yr), '') as fac_operated_this_yr,
        {{ state_date('sources.fac_op_per_begin_dt') }} as period_begin_date,
        {{ state_date('sources.fac_op_per_end_dt') }} as period_end_date,
        {%- for column in hcai_util_numeric_columns() %}
        {{ state_number('sources.' ~ column) }} as {{ column }},
        {%- endfor %}
        to_json(map_from_entries(list_filter([
            {%- for column in hcai_util_numeric_columns() %}
            {{ state_nulled_entry(column, 'sources.' ~ column) }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        ], entry -> entry.published is not null)))::varchar as nulled_values_published,
        files.latest_publication_date as publication_date,
        files.latest_download_run
    from sources
    left join files on sources._member_sha256 = files.member_sha256
    where {{ hcai_util_hold_reason('sources') }} is null
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
    left join matches on typed.fac_no = matches.facility_key
)

select
    * exclude (latest_download_run),
    member_sha256 || ':' || source_row_number as util_row_key,
    year(period_end_date) as report_year,
    dense_rank() over (
        partition by fac_no, period_begin_date, period_end_date, response_status
        order by publication_date desc nulls last, latest_download_run desc nulls last, member_sha256 desc
    ) = 1 as is_latest_release
from joined
