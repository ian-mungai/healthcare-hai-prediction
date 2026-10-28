-- One row per int_cmi_hospital_rows row, grain cmi_row_key: every silver column unchanged, then the surrogate keys.
{{
    gold_fact(
        silver_model='int_cmi_hospital_rows',
        grain='cmi_row_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
