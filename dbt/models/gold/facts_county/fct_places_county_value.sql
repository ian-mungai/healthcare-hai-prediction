-- fct_places_county_value: every column of int_places_county_values unchanged, then the surrogate keys county_sk, release_sk.
-- The grain is places_row_key; the YAML checks rows, content, keys, source dates and published text against silver.
{{
    gold_fact(
        'int_places_county_values',
        'places_row_key',
        {
            'county_sk': {'dimension': 'dim_county', 'natural_key': 'county_fips'},
            'release_sk': {'dimension': 'dim_release', 'natural_key': 'member_sha256'},
        },
    )
}}
