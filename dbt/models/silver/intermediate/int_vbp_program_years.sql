-- One Hospital VBP Total Performance Score row per hospital and program fiscal year from the latest stored release; files
-- without a fiscal_year take their reviewed year from vbp_file_fiscal_years; conflicts are held in
-- int_validation_program_holds (failure modes 514 to 516).
{{ care_program_years(
    'cms_cc_hvbp_tps',
    'coalesce(stg.facility_id, stg.provider_number)',
    vbp_fiscal_year(),
    vbp_program_columns()
) }}
