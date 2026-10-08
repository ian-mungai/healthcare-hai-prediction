-- Fails for each Hospital VBP Total Performance Score file that has neither a fiscal_year nor a reviewed year in
-- vbp_file_fiscal_years, so no program year is guessed [515].
select distinct
    stg._member_sha256 as member_sha256,
    stg._member_path as member_path
from {{ ref('stg_cms_cc_hvbp_tps') }} as stg
left join {{ ref('vbp_file_fiscal_years') }} as reviewed on stg._member_path = reviewed.file_name
where try_cast(trim(stg.fiscal_year) as integer) is null and reviewed.fiscal_year is null
