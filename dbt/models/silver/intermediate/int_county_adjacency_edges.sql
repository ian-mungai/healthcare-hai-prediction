-- One row per county and neighbor in each Census adjacency vintage: the 2023 to 2026 CSVs and the 2010 text file, whose
-- continuation lines take the county of the lead line above them (a lead line without a name still counts). Self-links
-- are flagged, an island keeps one row with no neighbor, and the shared border length is typed in meters where published.
-- No edge is removed or deduplicated: graph rules are later work (failure modes 400, 402, 408 and 409).
{{ config(materialized='table') }}

with

csv_rows as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        county_name,
        county_geoid,
        neighbor_name,
        neighbor_geoid,
        length
    from {{ ref('stg_county_adjacency') }}
),

text_rows as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        string_split(line_text, chr(9)) as line_fields
    from {{ ref('stg_county_adjacency_2010_text_lines') }}
),

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table in ('county_adjacency', 'county_adjacency_2010_text_lines')
),

text_marked as (
    select
        member_sha256,
        source_row_number,
        line_fields,
        max(case when nullif(trim(line_fields[2]), '') is not null then source_row_number end) over (
            partition by member_sha256 order by source_row_number rows between unbounded preceding and current row
        ) as lead_row_number
    from text_rows
),

text_edges as (
    select
        line_rows.member_sha256,
        line_rows.source_row_number,
        {{ unquoted('leads.line_fields[1]') }} as county_name,
        leads.line_fields[2] as county_published,
        {{ unquoted('line_rows.line_fields[3]') }} as neighbor_name,
        line_rows.line_fields[4] as neighbor_published,
        null::varchar as length_published
    from text_marked as line_rows
    left join text_marked as leads
        on
            line_rows.member_sha256 = leads.member_sha256
            and line_rows.lead_row_number = leads.source_row_number
),

csv_edges as (
    select
        member_sha256,
        source_row_number,
        {{ unquoted('county_name') }} as county_name,
        county_geoid as county_published,
        {{ unquoted('neighbor_name') }} as neighbor_name,
        neighbor_geoid as neighbor_published,
        length as length_published
    from csv_rows
),

edges as (
    select * from csv_edges
    union all
    select * from text_edges
),

typed as (
    select
        edges.member_sha256,
        edges.source_row_number,
        periods.vintage,
        edges.county_name,
        edges.county_published,
        {{ county_fips('edges.county_published') }} as county_fips,
        edges.neighbor_name,
        edges.neighbor_published,
        {{ county_fips('edges.neighbor_published') }} as neighbor_fips,
        edges.length_published,
        {{ strict_number('edges.length_published') }} as shared_border_length_m
    from edges
    left join periods on edges.member_sha256 = periods.member_sha256
)

select
    member_sha256,
    source_row_number,
    vintage,
    county_fips,
    county_name,
    county_published,
    neighbor_fips,
    neighbor_name,
    neighbor_published,
    shared_border_length_m,
    length_published,
    {{ county_scope('county_fips') }} as county_scope,
    member_sha256 || ':' || source_row_number as edge_key,
    coalesce(county_fips = neighbor_fips, false) as is_self_link,
    nullif(trim(neighbor_published), '') is null as is_isolated,
    left(county_fips, 2) = '09' as is_connecticut
from typed
