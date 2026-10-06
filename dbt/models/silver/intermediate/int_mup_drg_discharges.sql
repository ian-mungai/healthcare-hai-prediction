-- One row per Medicare inpatient provider (CCN), DRG and file: the data and release years from the file name and the
-- published discharges and averages typed. Cells under 11 discharges are not published (failure modes 355, 356, 359, 362).
{{ config(materialized='table') }}

with

cells as (
    select
        _member_sha256 as member_sha256,
        _member_path as member_path,
        nullif(trim(rndrng_prvdr_ccn), '') as ccn,
        nullif(trim(drg_cd), '') as drg_cd,
        nullif(trim(drg_desc), '') as drg_desc,
        try_cast(regexp_extract(lower(_member_path), 'dy([0-9]{2})', 1) as integer) + 2000 as data_year,
        try_cast(regexp_extract(lower(_member_path), 'ry([0-9]{2})', 1) as integer) + 2000 as release_year,
        {{ strict_number('tot_dschrgs') }} as tot_dschrgs,
        {{ strict_number('avg_submtd_cvrd_chrg') }} as avg_submtd_cvrd_chrg,
        {{ strict_number('avg_tot_pymt_amt') }} as avg_tot_pymt_amt,
        {{ strict_number('avg_mdcr_pymt_amt') }} as avg_mdcr_pymt_amt
    from {{ ref('stg_cms_medicare_inpatient_by_drg') }}
)

select
    *,
    member_sha256 || ':' || ccn || ':' || drg_cd as drg_key
from cells
