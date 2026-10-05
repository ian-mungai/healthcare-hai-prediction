-- Fails for a workbook sheet whose status and selection disagree, or that is covered by a text intermediate models
-- cannot read: covered sheets need a covering text that is selected (failure modes 268, 273, 278).
with

sheets as (
    select
        sheet_key,
        sheet_status,
        covering_sha256,
        is_selected
    from {{ ref('stg_bronze__sheet_selection') }}
),

texts as (
    select
        member_sha256,
        is_selected
    from {{ ref('stg_bronze__file_selection') }}
    where bronze_table in ('cms_ipps_text_lines', 'cms_occupational_mix_text_lines', 'cms_occupational_mix_text_lines_utf16')
)

select
    sheets.sheet_key,
    sheets.sheet_status
from sheets
left join texts on sheets.covering_sha256 = texts.member_sha256
where
    (sheets.sheet_status = 'covered' and not coalesce(texts.is_selected, false))
    or (sheets.sheet_status <> 'covered' and sheets.covering_sha256 is not null)
    or (sheets.sheet_status in ('file_selected', 'kept') and not sheets.is_selected)
    or (sheets.sheet_status in ('covered', 'twin_differs', 'label_held') and sheets.is_selected)
