-- One row per int_change_of_ownership_holds row, grain hold_key: every silver column unchanged, then the surrogate keys.
{{
    gold_fact(
        silver_model='int_change_of_ownership_holds',
        grain='hold_key',
        surrogate_keys={},
    )
}}
