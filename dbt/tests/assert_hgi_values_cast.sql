-- Fails for each Hospital General Information value that is not blank and does not fit its column: an overall rating
-- other than 1 to 5 or Not Available, or an emergency-services value other than Yes or No [329].
with

hgi_rows as (
    select
        hospital_overall_rating as rating,
        emergency_services as emergency
    from {{ ref('stg_cms_cc_hospital_general_information') }}
)

select
    'hospital_overall_rating' as bronze_column,
    rating as published_value,
    count(*) as row_count
from hgi_rows
where
    trim(rating) <> ''
    and trim(rating) not in ('1', '2', '3', '4', '5', 'Not Available')
group by rating
union all
select
    'emergency_services' as bronze_column,
    emergency as published_value,
    count(*) as row_count
from hgi_rows
where
    trim(emergency) <> ''
    and upper(trim(emergency)) not in ('YES', 'NO')
group by emergency
