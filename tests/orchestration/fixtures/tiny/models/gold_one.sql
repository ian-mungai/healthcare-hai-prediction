{{ config(tags=['gold']) }}
select id from {{ ref('silver_two') }}
