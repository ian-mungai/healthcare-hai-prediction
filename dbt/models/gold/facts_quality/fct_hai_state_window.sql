{{
    gold_fact(
        silver_model='int_hai_state_windows',
        grain='window_key',
        surrogate_keys={
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'state_sk': {'dimension': 'dim_state', 'natural_key': 'entity_id'},
        },
    )
}}
