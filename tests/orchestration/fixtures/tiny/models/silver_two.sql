{{ config(tags=['silver']) }}
select id from {{ ref('silver_one') }}
