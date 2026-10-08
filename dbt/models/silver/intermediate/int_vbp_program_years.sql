-- One Hospital VBP Total Performance Score row per hospital and program fiscal year from the latest stored release, with
-- every published domain score; files without a fiscal_year take their reviewed year from vbp_file_fiscal_years; conflicts
-- and malformed hospital IDs are held in int_validation_program_holds (failure modes 514 to 516, 524 and 525).
{{ care_program_years(
    'cms_cc_hvbp_tps',
    validation_ccn('coalesce(stg.facility_id, stg.provider_number)'),
    vbp_fiscal_year(),
    vbp_program_columns()
) }}
