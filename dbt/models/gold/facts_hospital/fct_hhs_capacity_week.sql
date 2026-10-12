-- One row per int_hhs_capacity_weeks row, grain week_key: every silver column unchanged, then the surrogate keys [743] [745].
{{
    gold_fact(
        silver_model='int_hhs_capacity_weeks',
        grain='week_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'state_sk': {'dimension': 'dim_state', 'natural_key': 'state'},
            'zip_sk': {'dimension': 'dim_zip', 'natural_key': 'zip'},
        },
    )
}}
