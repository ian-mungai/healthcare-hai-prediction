-- One row per int_onc_attestation_rows row, grain attestation_key: every silver column unchanged, then the surrogate keys [743] [745].
{{
    gold_fact(
        silver_model='int_onc_attestation_rows',
        grain='attestation_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'zip_sk': {'dimension': 'dim_zip', 'natural_key': 'zip'},
        },
    )
}}
