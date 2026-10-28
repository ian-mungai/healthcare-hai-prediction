-- One row per int_change_of_ownership_rows row, grain event_key: every silver column unchanged, then the surrogate keys.
{{
    gold_fact(
        silver_model='int_change_of_ownership_rows',
        grain='event_key',
        surrogate_keys={
            'enrollment_buyer_sk': {'dimension': 'dim_enrollment', 'natural_key': 'enrollment_id_buyer'},
            'enrollment_seller_sk': {'dimension': 'dim_enrollment', 'natural_key': 'enrollment_id_seller'},
            'hospital_buyer_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn_buyer'},
            'hospital_seller_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn_seller'},
            'owner_organization_buyer_sk': {'dimension': 'dim_owner_organization', 'natural_key': 'associate_id_buyer'},
            'owner_organization_seller_sk': {'dimension': 'dim_owner_organization', 'natural_key': 'associate_id_seller'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
