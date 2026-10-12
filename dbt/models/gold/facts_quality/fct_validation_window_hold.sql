{{
    gold_fact(
        silver_model='int_validation_window_holds',
        grain='hold_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'entity_id'},
        },
    )
}}
