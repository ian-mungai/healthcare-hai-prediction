{{ config(materialized='table') }}

select *
from {{ ref('bridge_measure_control') }}
where source_seed = 'county_health_measures'
