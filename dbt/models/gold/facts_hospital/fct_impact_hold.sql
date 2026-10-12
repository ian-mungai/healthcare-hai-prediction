-- One row per int_impact_holds row, grain hold_key: every silver column unchanged, then the surrogate keys [743] [745].
{{
    gold_fact(
        silver_model='int_impact_holds',
        grain='hold_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
