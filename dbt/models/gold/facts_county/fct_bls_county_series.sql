-- fct_bls_county_series: every column of int_bls_county_series unchanged, then the surrogate keys county_sk, release_sk.
-- The grain is bls_row_key; the YAML checks rows, content, keys, source dates and published text against silver.
{{
    gold_fact(
        'int_bls_county_series',
        'bls_row_key',
        {
            'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
