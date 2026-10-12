-- fct_gv_county_value: every column of int_gv_county_values unchanged, then the surrogate keys county_sk, release_sk [743] [745].
-- The grain is gv_value_key; the YAML checks rows, content, keys, source dates and published text against silver [747] [748].
{{
    gold_fact(
        'int_gv_county_values',
        'gv_value_key',
        {
            'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
