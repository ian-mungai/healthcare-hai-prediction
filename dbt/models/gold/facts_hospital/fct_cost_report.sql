-- One row per int_cost_reports row, grain rpt_rec_num: every silver column unchanged, then the surrogate keys [743] [745].
{{
    gold_fact(
        silver_model='int_cost_reports',
        grain='rpt_rec_num',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'state_sk': {'dimension': 'dim_state', 'natural_key': 'state_code'},
            'zip_sk': {'dimension': 'dim_zip', 'natural_key': 'zip_code'},
        },
    )
}}
