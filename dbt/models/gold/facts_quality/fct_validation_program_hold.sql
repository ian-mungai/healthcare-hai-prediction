{{
    gold_fact(
        silver_model='int_validation_program_holds',
        grain='hold_key',
        surrogate_keys={
            'hospital_sk': {'dimension': 'dim_hospital', 'natural_key': 'ccn'},
        },
    )
}}
