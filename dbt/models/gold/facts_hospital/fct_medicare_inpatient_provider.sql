-- One row per int_mup_providers row, grain provider_key: every silver column unchanged, then the surrogate keys.
{{
    gold_fact(
        silver_model='int_mup_providers',
        grain='provider_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'state_sk': {'dimension': 'dim_state', 'natural_key': 'state_code'},
        },
    )
}}
