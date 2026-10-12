-- fct_ruca_code: every column of int_ruca_codes unchanged, then the surrogate keys county_sk, release_sk [743] [745].
-- The grain is ruca_row_key; the YAML checks rows, content, keys, source dates and published text against silver [747] [748].
{{
    gold_fact(
        'int_ruca_codes',
        'ruca_row_key',
        {
            'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
