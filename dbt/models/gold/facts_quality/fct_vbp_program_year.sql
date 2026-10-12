{{
    gold_fact(
        silver_model='int_vbp_program_years',
        grain='program_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
