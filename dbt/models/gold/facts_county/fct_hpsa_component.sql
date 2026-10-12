-- fct_hpsa_component: every column of int_hpsa_components unchanged, then the surrogate keys county_sk, release_sk [743] [745].
-- The grain is hpsa_row_key; the YAML checks rows, content, keys, source dates and published text against silver [747] [748].
{{
    gold_fact(
        'int_hpsa_components',
        'hpsa_row_key',
        {
            'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
