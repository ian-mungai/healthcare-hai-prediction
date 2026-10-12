-- fct_mmd_prevalence: every column of int_mmd_prevalence unchanged, then the surrogate keys county_sk, release_sk [743] [745].
-- The grain is mmd_row_key; the YAML checks rows, content, keys, source dates and published text against silver [747] [748].
{{
    gold_fact(
        'int_mmd_prevalence',
        'mmd_row_key',
        {
            'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
