-- fct_hsa_zip_cases: every column of int_hsa_zip_cases unchanged, then the surrogate keys release_sk, zip_sk.
-- The grain is hsa_row_key; the YAML checks rows, content, keys, source dates and published text against silver.
{{
    gold_fact(
        'int_hsa_zip_cases',
        'hsa_row_key',
        {
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'zip_sk': {'dimension': 'dim_zip', 'natural_key': 'zip_code'},
        },
    )
}}
