-- HAC Reduction and VBP rows the program-year models leave out, with the reason, row count, hold key and the sorted checksums
-- of the files they come from (failure modes 514, 523, 524 and 788).
select * from ({{ care_program_holds(
    'cms_cc_hac_reduction_program_hospital',
    validation_ccn('stg.facility_id'),
    'try_cast(trim(stg.fiscal_year) as integer)',
    hac_program_columns()
) }})
union all by name
select * from ({{ care_program_holds(
    'cms_cc_hvbp_tps',
    validation_ccn('coalesce(stg.facility_id, stg.provider_number)'),
    vbp_fiscal_year(),
    vbp_program_columns()
) }})
