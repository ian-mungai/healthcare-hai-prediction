-- fct_rucc_county: every column of int_rucc_county_codes unchanged, then the surrogate keys county_sk, release_sk, state_sk.
-- The grain is rucc_key; the YAML checks rows, content, keys, source dates and published text against silver.
{{
    gold_fact(
        'int_rucc_county_codes',
        'rucc_key',
        {
            'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
            'state_sk': {'dimension': 'dim_state', 'natural_key': 'state'},
        },
    )
}}
