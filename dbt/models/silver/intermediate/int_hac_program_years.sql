-- One HAC Reduction Program row per hospital and fiscal year from the latest stored release; a revised file supersedes the
-- original; conflicts are held in int_validation_program_holds. The republished HAI SIRs are not staged; the HAI outcome
-- comes only from the HAI windows (failure modes 514 to 518).
{{ care_program_years(
    'cms_cc_hac_reduction_program_hospital',
    'stg.facility_id',
    'try_cast(trim(stg.fiscal_year) as integer)',
    hac_program_columns()
) }}
