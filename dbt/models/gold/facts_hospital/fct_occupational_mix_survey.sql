-- One row per int_occmix_survey_rows row, grain survey_row_key: every silver column unchanged, then the surrogate keys.
{{
    gold_fact(
        silver_model='int_occmix_survey_rows',
        grain='survey_row_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
