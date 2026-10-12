-- One row per int_hospital_owner_rows row, grain owner_row_key: every silver column unchanged, then the surrogate keys [743] [745].
{{
    gold_fact(
        silver_model='int_hospital_owner_rows',
        grain='owner_row_key',
        surrogate_keys={
            'enrollment_sk': {'dimension': 'dim_enrollment', 'natural_key': 'enrollment_id'},
            'owner_organization_sk': {'dimension': 'dim_owner_organization', 'natural_key': 'associate_id'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
