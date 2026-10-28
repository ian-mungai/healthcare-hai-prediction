-- One row per int_mup_drg_discharges row, grain drg_key: every silver column unchanged, then the surrogate keys.
{{
    gold_fact(
        silver_model='int_mup_drg_discharges',
        grain='drg_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
