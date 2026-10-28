{{ config(materialized='table') }}

select *
from {{ ref('bridge_measure_control') }}
where source_seed = 'shortage_measures'
