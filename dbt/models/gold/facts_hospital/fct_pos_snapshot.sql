-- One row per int_pos_hospital_snapshots row, grain snapshot_key: every silver column unchanged, then the surrogate keys [743] [745].
{{
    gold_fact(
        silver_model='int_pos_hospital_snapshots',
        grain='snapshot_key',
        surrogate_keys={
            'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'},
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'state_sk': {'dimension': 'dim_state', 'natural_key': 'state_code'},
            'zip_sk': {'dimension': 'dim_zip', 'natural_key': 'zip_code'},
        },
    )
}}
