-- One row per int_hospital_enrollment_rows row, grain enrollment_key: every silver column unchanged, then the surrogate keys [743] [745].
{{
    gold_fact(
        silver_model='int_hospital_enrollment_rows',
        grain='enrollment_key',
        surrogate_keys={
            'enrollment_sk': {'dimension': 'dim_enrollment', 'natural_key': 'enrollment_id'},
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
            'owner_organization_sk': {'dimension': 'dim_owner_organization', 'natural_key': 'associate_id'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'state_sk': {'dimension': 'dim_state', 'natural_key': 'state'},
            'zip_sk': {'dimension': 'dim_zip', 'natural_key': 'zip_code'},
        },
    )
}}
