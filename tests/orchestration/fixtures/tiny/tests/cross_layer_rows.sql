{{ config(tags=['cross_checks']) }}
select 1
where (select count(*) from {{ ref('gold_one') }}) != (select count(*) from {{ ref('silver_two') }})
